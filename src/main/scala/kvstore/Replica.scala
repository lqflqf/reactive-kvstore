package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{AskTimeoutException, ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.concurrent.{Await, Future}

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

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher


  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  //expected sequence number
  var _seqExpected = 0L

  //Persistence actor
  val persistence = context.system.actorOf(persistenceProps)

  //replicator map
  var repMap = Map.empty[Long, Set[ActorRef]]

  //cancel map
  var cancelMmap = Map.empty[Long, Cancellable]

  //reply map
  var senderMap = Map.empty[Long, ActorRef]


  arbiter ! Join

  override val supervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }

  implicit val timeout = Timeout(1 second)


  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {


    case Replicas(replicas) =>

      // add replica
      for (d <- replicas.-(self).diff(secondaries.keySet)) {
        val rep = context.system.actorOf(Replicator.props(d))
        secondaries += d -> rep
        replicators += rep

        for ((k, v) <- kv) rep ! Replicate(k, Some(v), 0L)

      }

      // remove replica
      for (d <- secondaries.keySet.diff(replicas.-(self))) {
        val rep = secondaries(d)
        rep ! PoisonPill
        secondaries -= d
        replicators -= rep

        repMap.foreach {
          i =>
            if (i._2.contains(rep)) {
              val v2 = i._2.-(rep)
              if (v2.isEmpty) {
                repMap -= i._1
                if (cancelMmap.get(i._1).isEmpty) {
                  val s = senderMap(i._1)
                  senderMap -= i._1
                  s ! OperationAck(i._1)
                }
              }
              else {
                repMap += i._1 -> v2
              }
            }
        }
      }


    case i: Insert =>
      (self ? Replicate(i.key, Some(i.value), i.id)).recover {
        case _: AskTimeoutException => OperationFailed(i.id)
      }.pipeTo(sender())

    case r: Remove =>
      (self ? Replicate(r.key, None, r.id)).recover {
        case _: AskTimeoutException => OperationFailed(r.id)
      }.pipeTo(sender())

    case g: Get => sender() ! GetResult(g.key, kv.get(g.key), g.id)

    case r: Replicate =>
      r.valueOption match {
        case Some(x) => kv += r.key -> x
        case None => kv -= r.key
      }
      
      senderMap += r.id -> sender()

      if (!replicators.isEmpty) {
        replicators.foreach(_ ! r)
        repMap += r.id -> replicators
      }

      val c =
        context.system.scheduler.schedule(0 millisecond, 100 milliseconds, persistence, Persist(r.key, r.valueOption, r.id))

      cancelMmap += r.id -> c

    case p: Persisted =>
      val c = cancelMmap(p.id)
      cancelMmap -= p.id
      c.cancel()
      if (repMap.get(p.id).isEmpty) {
        val s = senderMap(p.id)
        senderMap -= p.id
        s ! OperationAck(p.id)
      }

    case re: Replicated =>
      if (repMap.contains(re.id)) {
        var rep_set = repMap(re.id)
        rep_set -= sender()
        if (rep_set.isEmpty) {
          repMap -= re.id
          if (cancelMmap.get(re.id).isEmpty) {
            val s = senderMap(re.id)
            senderMap -= re.id
            s ! OperationAck(re.id)
          }

        } else {
          repMap += re.id -> rep_set
        }
      }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case g: Get => sender() ! GetResult(g.key, kv.get(g.key), g.id)
    case s: Snapshot =>
      if (s.seq == _seqExpected) {
        s.valueOption match {
          case Some(x) => kv += s.key -> x
          case None => kv -= s.key
        }
        _seqExpected += 1

        senderMap += s.seq -> sender()

        val c =
          context.system.scheduler.schedule(0 millisecond, 100 milliseconds, persistence, Persist(s.key, s.valueOption, s.seq))

        cancelMmap += s.seq -> c
      }
      else if (s.seq < _seqExpected) {
        sender() ! SnapshotAck(s.key, s.seq)
      }
    case p: Persisted =>
      val c = cancelMmap(p.id)
      cancelMmap -= p.id
      c.cancel()
      val s = senderMap(p.id)
      senderMap -= p.id
      s ! SnapshotAck(p.key, p.id)

  }


}

