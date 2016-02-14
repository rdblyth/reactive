package kvstore

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.MILLISECONDS
import Persistence.Persist
import Persistence.Persisted
import Replicator.Snapshot
import Replicator.SnapshotAck
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.persistence.AtLeastOnceDelivery
import kvstore.Arbiter.Join
import kvstore.Arbiter.JoinedPrimary
import kvstore.Arbiter.JoinedSecondary
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import akka.actor.Cancellable
import kvstore.Arbiter.Replicas
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with AtLeastOnceDelivery {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  arbiter ! Join

  val persistence: ActorRef = context.actorOf(persistenceProps)

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persistAck = Map.empty[Long, (ActorRef, Cancellable)]
  var replicateAck = Map.empty[Long, Set[ActorRef]]
  var failure  = Map.empty[Long, Cancellable]

  var expectedSeq = 0;

  override def redeliverInterval: FiniteDuration = {
    FiniteDuration(100, MILLISECONDS)
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case ex: PersistenceException => Resume
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Replicas(replicas) => {
      val newReplicas = replicas.filter { replica => !secondaries.contains(replica) && replica != self }
      val newReplicators = newReplicas.map { replica => context.actorOf(Replicator.props(replica)) }

      val removedReplicas = secondaries.filterKeys { replica => !replicas.contains(replica) }.keySet
      val removedReplicators = replicators.filter { replicator => !replicas.contains(replicator) }

      replicateAck.foreach(x => {
        replicateAck = replicateAck + (x._1 -> x._2.filter { actor => !removedReplicators.contains(actor) })
      })

      replicateAck.foreach(x => {
        val id = x._1
        if (x._2.isEmpty && persistAck(id)._2.isCancelled) {
          failure.get(id).get.cancel()
          persistAck.get(id).get._1 ! OperationAck(id)
          persistAck = persistAck - id
          replicateAck = replicateAck - id
          failure = failure - id
        }
      })

      secondaries = secondaries -- removedReplicas

      for (newReplica <- newReplicas; newReplicator <- newReplicators) {
        secondaries = secondaries + (newReplica -> newReplicator)
      }

      removedReplicators.foreach {
        replicator => context.stop(replicator)
      }

      var id = -1
      for (pair <- kv; replicator <- newReplicators) {
        replicator ! Replicate(pair._1, Option(pair._2), id)
        id = id - 1
      }

      replicators = replicators -- removedReplicators
      replicators = replicators ++ newReplicators
    }
    case Insert(key, value, id) => {
      println(s"Got Insert message for id=$id")
      kv = kv + (key -> value)
      val cancellable = context.system.scheduler.scheduleOnce(Duration.create(1, TimeUnit.SECONDS), sender, OperationFailed(id))

      failure = failure + (id -> cancellable)

      replicateAck = replicateAck + (id -> replicators)
      replicators.foreach { replicator => replicator ! Replicate(key, Option(value), id) }

      val cancellable2 = context.system.scheduler.schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(100, TimeUnit.MILLISECONDS))(persistence ! Persist(key, Option(value), id))

      persistAck = persistAck + (id -> (sender, cancellable2))
    }
    case Remove(key, id) => {
      println(s"Got Remove message for id=$id and key=$key")
      kv = kv - key

      val cancellable = context.system.scheduler.scheduleOnce(Duration.create(1, TimeUnit.SECONDS), sender, OperationFailed(id))

      failure = failure + (id -> cancellable)

      replicateAck = replicateAck + (id -> replicators)
      replicators.foreach { replicator => replicator ! Replicate(key, None, id) }

      val cancellable2 = context.system.scheduler.schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(100, TimeUnit.MILLISECONDS))(persistence ! Persist(key, None, id))

      persistAck = persistAck + (id -> (sender, cancellable2))
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Persisted(key, id) => {
      println(s"Got Persisted confirmation for id=id")
      persistAck.get(id).get._2.cancel()

      if (replicateAck.get(id).get.isEmpty) {
        failure.get(id).get.cancel()
        persistAck.get(id).get._1 ! OperationAck(id)
        persistAck = persistAck - id
        replicateAck = replicateAck - id
        failure = failure - id
      }
    }
    case Replicated(key, id) => {
      if (id >= 0) {
        replicateAck = replicateAck + (id -> (replicateAck.get(id).get - sender))
        if (persistAck.get(id).get._2.isCancelled && replicateAck.get(id).get.isEmpty) {
          failure.get(id).get.cancel()
          persistAck.get(id).get._1 ! OperationAck(id)
          persistAck = persistAck - id
          replicateAck = replicateAck - id
          failure = failure - id
        }
      }
    }
    case msg => println(s"Received unsupported message $msg")
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Snapshot(key, valueOption, seq) => {
      println(s"Got Snapshot Message with seq=$seq")
      if (seq < expectedSeq) {
        println(s"Sending SnapshotAck for previous seq=$seq")
        sender ! SnapshotAck(key, seq)
      } else if (seq == expectedSeq) {
        if (valueOption == None) {
          kv = kv - key
        } else {
          kv = kv + (key -> valueOption.get)
        }

        val cancellable = context.system.scheduler.schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(100, TimeUnit.MILLISECONDS))(persistence ! Persist(key, valueOption, seq))

        persistAck = persistAck + (seq -> (sender, cancellable))
      }
    }
    case Persisted(key, seq) => {
      println(s"Got Persisted confirmation for seq=seq")
      persistAck.get(seq).get._2.cancel()
      persistAck.get(seq).get._1 ! SnapshotAck(key, seq)
      persistAck = persistAck - seq
      expectedSeq += 1
    }
    case msg => println(s"Replica received unsupported message $msg")
  }
}

