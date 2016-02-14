package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.persistence.AtLeastOnceDelivery
import java.util.concurrent.TimeUnit
import akka.actor.Cancellable

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var cancellables = Map.empty[Long, Cancellable]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) => {
      println(s"Got Replicate for id=$id and key=$key")
      val seq = nextSeq
      acks = acks + (seq -> (sender, Replicate(key, valueOption, id)))
      val cancellable = context.system.scheduler.schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(100, TimeUnit.MILLISECONDS))(replica ! Snapshot(key, valueOption, seq))
      cancellables = cancellables + (seq -> cancellable)
    }
    case SnapshotAck(key, seq) => {
      println(s"Got SnapshotAck for seq=$seq")
      cancellables.get(seq).get.cancel()
      val client = acks.get(seq).get._1
      val id = acks.get(seq).get._2.id
      client ! Replicated(key, id)
      acks = acks - seq
      cancellables = cancellables - seq
    }
    case msg => println(s"Replicator received unsupported message $msg")
  }
}
