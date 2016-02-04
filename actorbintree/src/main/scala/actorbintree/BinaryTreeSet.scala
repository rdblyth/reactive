/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._
import akka.event.Logging
import akka.event.LoggingReceive
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /**
    * Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
    * Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
    * Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /**
    * Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor {

  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  val log = Logging(context.system, this)

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(requestor, id, elem) => {
      log.debug(s"Receieved Insert with id=$id")
      root ! Insert(requestor, id, elem)
    }
    case Contains(requestor, id, elem) => {
      log.debug(s"Receieved Contains with id=$id")
      root ! Contains(requestor, id, elem)
    }
    case Remove(requestor, id, elem) => {
      log.debug(s"Receieved Remove with id=$id")
      root ! Remove(requestor, id, elem)
    }
    case GC => {
      log.debug(s"Receieved GC")
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /**
    * Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case Insert(requestor, id, elem) => {
      pendingQueue = pendingQueue :+ Insert(requestor, id, elem)
    }
    case Contains(requestor, id, elem) => {
      pendingQueue = pendingQueue :+ Contains(requestor, id, elem)
    }
    case Remove(requestor, id, elem) => {
      pendingQueue = pendingQueue :+ Remove(requestor, id, elem)
    }
    case GC => {
    }
    case CopyFinished => {
      root = newRoot
      pendingQueue.foreach { operation => root ! operation }
      pendingQueue = Queue.empty[Operation]
    }
  }
}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  val log = Logging(context.system, this)

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = LoggingReceive {
    case Insert(requestor, id, elem) => {
      if (this.elem == elem) {
        log.debug(s"Sending OperationFinished for id=$id")
        requestor ! OperationFinished(id)
      } else if (elem < this.elem) {
        if (subtrees.contains(Left)) {
          subtrees.get(Left).get ! Insert(requestor, id, elem)
        } else {
          log.debug(s"Inserting $elem into the Left")
          subtrees = subtrees + (Left -> context.actorOf(BinaryTreeNode.props(elem, false)))
          log.debug(s"Sending OperationFinished for id=$id")
          requestor ! OperationFinished(id)
        }
      } else {
        if (subtrees.contains(Right)) {
          subtrees.get(Right).get ! Insert(requestor, id, elem)
        } else {
          log.debug(s"Inserting $elem into the Right")
          subtrees = subtrees + (Right -> context.actorOf(BinaryTreeNode.props(elem, false)))
          log.debug(s"Sending OperationFinished for id=$id")
          requestor ! OperationFinished(id)
        }
      }
    }
    case Contains(requestor, id, elem) => {
      if (this.elem == elem) {
        if (removed) {
          log.debug(s"Sending ContainsResult with false for id=$id")
          requestor ! ContainsResult(id, false)
        } else {
          log.debug(s"Sending ContainsResult with true for id=$id")
          requestor ! ContainsResult(id, true)
        }
      } else if (elem < this.elem) {
        if (subtrees.contains(Left)) {
          subtrees.get(Left).get ! Contains(requestor, id, elem)
        } else {
          log.debug(s"Sending ContainsResult with false for id=$id")
          requestor ! ContainsResult(id, false)
        }
      } else {
        if (subtrees.contains(Right)) {
          subtrees.get(Right).get ! Contains(requestor, id, elem)
        } else {
          log.debug(s"Sending ContainsResult with false for id=$id")
          requestor ! ContainsResult(id, false)
        }
      }
    }
    case Remove(requestor, id, elem) => {
      if (this.elem == elem) {
        removed = true
        log.debug(s"Sending OperationFinished for id=$id")
        requestor ! OperationFinished(id)
      } else if (elem < this.elem) {
        if (subtrees.contains(Left)) {
          subtrees.get(Left).get ! Remove(requestor, id, elem)
        } else {
          log.debug(s"Sending OperationFinished for id=$id")
          requestor ! OperationFinished(id)
        }
      } else {
        if (subtrees.contains(Right)) {
          subtrees.get(Right).get ! Remove(requestor, id, elem)
        } else {
          log.debug(s"Sending OperationFinished for id=$id")
          requestor ! OperationFinished(id)
        }
      }
    }
    case CopyTo(newRoot) => {}
  }

  // optional
  /**
    *
    * `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???
}