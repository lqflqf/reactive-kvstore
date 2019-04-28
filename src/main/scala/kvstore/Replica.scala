package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{AskTimeoutException, ask, pipe}
import akka.actor.Terminated

import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

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

  arbiter ! Join

  override val supervisorStrategy = OneForOneStrategy(){
    case _: PersistenceException => Restart
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {


    case Replicas(replicas) =>
      for (d <- secondaries.keySet.diff(replicas)) {
        val rep = context.system.actorOf(Replicator.props(d))
        secondaries += d->rep
        replicators += rep
      }

    case i: Insert =>
      self.ask(Replicate(i.key, Some(i.value), i.id))(1 second).recover{
        case _: AskTimeoutException => OperationFailed(i.id)
      }.pipeTo(sender())

    case r: Remove =>
     self.ask(Replicate(r.key, None, r.id))(1 second).recover{
        case _: AskTimeoutException => OperationFailed(r.id)
      }.pipeTo(sender())

    case g: Get => sender() ! GetResult(g.key, kv.get(g.key), g.id)

    case r: Replicate =>
      r.valueOption match {
        case Some(x) => kv += r.key->x
        case None => kv -= r.key
      }
      replicators.foreach(_ ! r)

      persistence ! Persist(r.key, r.valueOption, r.id)

      sender() ! OperationAck(r.id)

    case _ =>

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case g: Get => sender() ! GetResult(g.key, kv.get(g.key), g.id)
    case s: Snapshot =>
      if (s.seq == _seqExpected){
        s.valueOption match {
          case Some(x) => kv += s.key->x
          case None => kv -= s.key
        }
        _seqExpected += 1

        persistence ! Persist(s.key, s.valueOption, s.seq)

        sender() ! SnapshotAck(s.key, s.seq)
      }
      else if (s.seq < _seqExpected) {
        sender() ! SnapshotAck(s.key, s.seq)
      }
  }

}

