package com.fortyninemaps.relay

import java.time.Instant
import java.util
import java.util.concurrent.{ConcurrentLinkedQueue, SynchronousQueue}

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object LogData {
  import Data.Coord

  sealed trait Op
  case class AddPartitions(partitions: Seq[TopicPartition]) extends Op
  case class RevokePartitions(partitions: Seq[TopicPartition]) extends Op
  case class Read(timeout: Duration) extends Op
  case class Acknowledge(coord: Coord) extends Op

  // Messages that can be in the internal log topic
  sealed trait LogMessage
  // Means that the message has been sent to a client
  case class LogPending(coord: Coord, timeout: Instant, epoch: Int) extends LogMessage
  // Means that the client acknowledged processing
  case class LogAcknowledged(coord: Coord) extends LogMessage
  // Means that the given offset has been committed in the data partition
  case class LogCommitted(coord: Coord) extends LogMessage
}

object Relay {
  import Data._
  import LogData._

  private def isCaughtUp(offsets: Map[TopicPartition, Long], consumer: KafkaConsumer[_, _]): Boolean =
    consumer.endOffsets(consumer.assignment()).asScala.map { case (tp, offset) =>
      offset - offsets.getOrElse(tp, 0L)
    }.sum > 0

  val PollTimeout: java.time.Duration = ???

  private def deserializeLogMessage(rec: ConsumerRecord[Array[Byte], Array[Byte]]): Try[LogMessage] = ???

  // Insert a message into the internal state vector, potentially overwriting a previous version
  private def insertMessage(msg: Message)(partitionState: Vector[Message]): Vector[Message] =
    partitionState.partition(_.dataOffset < msg.dataOffset) match {
      case (before, after) if after.isEmpty => before :+ msg
      case (before, after) if after.headOption.exists(_.dataOffset == msg.dataOffset) => before ++ after.updated(0, msg)
      case (before, after) => (before :+ msg) ++ after
    }

  // Trim all committed messages from the internal state vector while committing to Kafka
  private def maybePrune(consumer: KafkaConsumer[Array[Byte], Array[Byte]], partition: TopicPartition, state: Vector[Message]): Vector[Message] = {
    val pruneBefore = state.indexWhere(_.state != MessageState.Committed)
    if (pruneBefore == 0) state
    else {
      val newState = state.drop(pruneBefore)
      // We only need back to the minimum offset in the current state. If we have updates remaining no lower than N,
      // we never need to see log messages up to N - 1 again.
      consumer.commitSync(
        newState.map(_.latestLog).groupBy(_.topicPartition).map { case (_, coords) =>
          coords.minBy(_.offset).offsetMap
        }.foldLeft(Map.empty[TopicPartition, OffsetAndMetadata])(_ ++ _).asJava
      )
      newState
    }
  }

  // Replay from the log topic until caught up, building up a representation of the global state
  @tailrec
  private def replayLog(
    consumer: KafkaConsumer[Array[Byte], Array[Byte]],
    haveReadTo: Map[TopicPartition, Long],
    state: Map[TopicPartition, Vector[Message]]
  ): Map[TopicPartition, Vector[Message]] =
    if (isCaughtUp(haveReadTo, consumer)) state
    else {
      val records = consumer.poll(PollTimeout).asScala
      val haveReadToNow = records.groupBy(r => (r.topic(), r.partition())).map { case ((t, p), rs) =>
        new TopicPartition(t, p) -> rs.map(_.offset()).max
      }
      val nextState = records.foldLeft(state) {
        case (st, record) => deserializeLogMessage(record) match {
          case Failure(_) => st // TODO: handle
          case Success(LogPending(crd, timeout, epoch)) =>
            st.updated(
              crd.topicPartition,
              st.get(crd.topicPartition)
                .map(insertMessage(Message(MessageState.Pending(timeout, None, epoch), dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
                .getOrElse(Vector(Message(MessageState.Pending(timeout, None, epoch), dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
            )
          case Success(LogAcknowledged(crd)) =>
            st.updated(
              crd.topicPartition,
              st.get(crd.topicPartition)
                .map(insertMessage(Message(MessageState.Acknowledged, dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
                .getOrElse(Vector(Message(MessageState.Acknowledged, dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
            )
          case Success(LogCommitted(crd)) =>
            // This one is different! We might prune the vector!
            st.updated(
              crd.topicPartition,
              maybePrune(
                consumer,
                crd.topicPartition,
                st.get(crd.topicPartition)
                  .map(insertMessage(Message(MessageState.Acknowledged, dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
                  .getOrElse(Vector(Message(MessageState.Acknowledged, dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
              )
            )
        }
      }

      replayLog(
        consumer,
        haveReadToNow,
        nextState
      )
    }

  // Handle incoming requests while writing to and reading from the log topic. We can only send out messages once we've
  // read our own write!
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
    val kafkaConfig: Map[String, AnyRef] = ???

    val requestQueue: ConcurrentLinkedQueue[Op] = ???
    val responseQueue: ConcurrentLinkedQueue[ConsumerRecord[Array[Byte], Array[Byte]]] = ???
    val rebalanceDone: SynchronousQueue[Unit] = ???

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaConfig.asJava)
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

