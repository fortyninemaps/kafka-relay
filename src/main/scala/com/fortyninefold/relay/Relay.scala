package com.fortyninefold.relay

import java.time.Instant
import java.util
import java.util.concurrent.{ConcurrentLinkedQueue, SynchronousQueue}

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object Data {
  sealed trait MessageState
  case class Pending(timeout: Instant) extends MessageState
  case object Acknowledged extends MessageState
  case class Message(state: MessageState, offset: Long)

  sealed trait Op
  case class AddPartitions(partitions: Seq[TopicPartition]) extends Op
  case class RevokePartitions(partitions: Seq[TopicPartition]) extends Op
  case class Read(timeout: Duration) extends Op
  case class Acknowledge(partition: TopicPartition, offset: Long) extends Op

  sealed trait LogMessage
  case class PendingBatch(messages: Seq[(TopicPartition, Long, Instant)]) extends LogMessage
  case class Acknowledged(partition: TopicPartition, offset: Long) extends LogMessage
}

object Relay {
  import Data._

  private def isCaughtUp(offsets: Map[TopicPartition, Long], consumer: KafkaConsumer[_, _]): Boolean =
    consumer.endOffsets(consumer.assignment()).asScala.map { case (tp, offset) =>
      offset - offsets.getOrElse(tp, 0L)
    }.sum > 0

  val PollTimeout: java.time.Duration = ???

  private def deserializeLogMessage(rec: ConsumerRecord[Array[Byte], Array[Byte]]): Try[LogMessage] = ???

  private def insertPending(offset: Long, timeout: Instant)(partitionState: Vector[Message]): Vector[Message] =
    partitionState.partition(_.offset < offset) match {
      case (before, after) => (before :+ Message(Pending(timeout), offset)) ++ after
    }

  private def insertAcknowledged(offset: Long)(partitionState: Vector[Message]): Vector[Message] =
    partitionState.partition(_.offset < offset) match {
      case (before, after) => (before :+ Message(Acknowledged, offset)) ++ after
    }

  @tailrec
  private def replayLog(
    consumer: KafkaConsumer[Array[Byte], Array[Byte]],
    hevaReadTo: Map[TopicPartition, Long],
    state: Map[TopicPartition, Vector[Message]]
  ): Map[TopicPartition, Vector[Message]] =
    if (isCaughtUp(hevaReadTo, consumer)) state
    else {
      val records = consumer.poll(PollTimeout).asScala
      val haveReadToNow = records.groupBy(r => (r.topic(), r.partition())).map { case ((t, p), rs) =>
        new TopicPartition(t, p) -> rs.map(_.offset()).max
      }
      val nextState = records.foldLeft(state) {
        case (st, record) => deserializeLogMessage(record) match {
          case Failure(_) => st // TODO: handle
          case Success(PendingBatch(messages)) => messages.foldLeft(st) { case (s, (tp, o, inst)) =>
            s.updated(
              tp,
              s.get(tp)
                .map(insertPending(o, inst))
                .getOrElse(Vector(Message(Pending(inst), o)))
            )
          }
          case Success(Acknowledged(partition, offset)) =>
            st.updated(
              partition,
              st.get(partition)
                .map(insertAcknowledged(offset))
                .getOrElse(Vector(Message(Acknowledged, offset)))
            )
        }
      }

      replayLog(
        consumer,
        haveReadToNow,
        nextState
      )
    }

  //@tailrec
  private def pollAndServe(
    consumer: KafkaConsumer[_, _],
    requests: ConcurrentLinkedQueue[Op],
    responses: ConcurrentLinkedQueue[ConsumerRecord[Array[Byte], Array[Byte]]],
    rebalanceDone: SynchronousQueue[Unit],
    state: Map[TopicPartition, Vector[Message]]
  ): Nothing = ???

  def run(port: Int, inputTopics: Set[String]): Unit = {

    val logTopics = Set("relay.log")
    val config: Map[String, AnyRef] = ???

    val requestQueue: ConcurrentLinkedQueue[Op] = ???
    val responseQueue: ConcurrentLinkedQueue[ConsumerRecord[Array[Byte], Array[Byte]]] = ???
    val rebalanceDone: SynchronousQueue[Unit] = ???


    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.asJava)
    consumer.subscribe(logTopics.asJava)

    // Catch up by reading the log topic
    val committedLogOffsets = consumer.committed(consumer.assignment()).asScala
      .map { case (k, v) => k -> v.offset() }.toMap
    val replayState: Map[TopicPartition, Vector[Message]] =
      replayLog(consumer, committedLogOffsets, Map.empty)


    // Start polling and serving requests
    consumer.subscribe((logTopics ++ inputTopics).asJava, new RebalanceListener(requestQueue, rebalanceDone))

    pollAndServe(consumer, requestQueue, responseQueue, rebalanceDone, replayState)

    ???
  }

  class RebalanceListener[K, V](requestQueue: ConcurrentLinkedQueue[Op], rebalanceDone: SynchronousQueue[Unit]) extends ConsumerRebalanceListener {
    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
      requestQueue.offer(AddPartitions(partitions.asScala.toSeq))
      rebalanceDone.take()
    }

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      requestQueue.offer(RevokePartitions(partitions.asScala.toSeq))
      rebalanceDone.take()
    }
  }

}

