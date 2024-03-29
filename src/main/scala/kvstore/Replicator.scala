package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props}

import scala.concurrent.duration._

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
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  var schedulerMap = Map.empty[Long, Cancellable]

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r: Replicate =>
      val id = nextSeq()
      acks = acks.+((id, (sender(), r)))

      val c =
      context.system.scheduler.schedule(0 millisecond, 100 milliseconds, replica, Snapshot(r.key, r.valueOption, id))

      schedulerMap += id->c

    case sa: SnapshotAck =>
      val (s, r) = acks(sa.seq)
      schedulerMap(sa.seq).cancel()
      acks -= sa.seq
      schedulerMap -= sa.seq
      s ! Replicated(r.key, r.id)
  }

}
