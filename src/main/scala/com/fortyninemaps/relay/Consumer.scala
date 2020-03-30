package com.fortyninemaps.relay

import java.util.concurrent.atomic.AtomicReference

import com.fortyninemaps.relay.Data.{Coord, Message, MessageState, PartitionState}
import com.fortyninemaps.relay.LogData.{LogAcknowledged, LogCommitted, LogMessage, LogPending}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// The consumer reads from both data and log topics, updating a copy of the state-map
object Consumer {
  type BytesConsumer = KafkaConsumer[Array[Byte], Array[Byte]]
  type BytesRecord = ConsumerRecord[Array[Byte], Array[Byte]]

  val PollTimeout: java.time.Duration = ???
  val PerPartitionAvailableTarget: Int = ???
  val PerPartitionAvailableMinimum: Int = ???
  val LogTopics: Set[String] = Set("relay.log")

  private def deserializeLogMessage(rec: ConsumerRecord[Array[Byte], Array[Byte]]): Try[LogMessage] = ???

  // Insert a message into the internal state vector, potentially overwriting a previous version
  // FIXME: this is a linear scan, but we could bisect
  private def insertMessage(msg: Message)(partitionState: PartitionState): PartitionState =
    partitionState.messages.partition(_.dataOffset < msg.dataOffset) match {
      case (before, after) if after.isEmpty =>
        PartitionState(before :+ msg, partitionState.deleted)
      case (before, after) if after.headOption.exists(_.dataOffset == msg.dataOffset) =>
        PartitionState(before ++ after.updated(0, msg), partitionState.deleted)
      case (before, after) =>
        PartitionState((before :+ msg) ++ after, partitionState.deleted)
    }

  // Trim all committed messages from the internal state vector while committing to Kafka
  private def prune(state: Map[TopicPartition, PartitionState]): (Map[TopicPartition, PartitionState], Map[TopicPartition, OffsetAndMetadata]) =
    state.foldLeft((Map.empty[TopicPartition, PartitionState], Map.empty[TopicPartition, OffsetAndMetadata])) {
      case ((totalState, offsets), (partition, partitionState)) => {
        val pruneBefore = partitionState.messages.indexWhere(_.state != MessageState.Committed)
        if (pruneBefore == 0)
          (totalState + (partition -> partitionState), offsets)
        else {
          val prunedMessages = partitionState.messages.drop(pruneBefore)
          val newDeletionMarker = partitionState.deleted + pruneBefore
          val commitOffset = prunedMessages.headOption.map(m => new OffsetAndMetadata(m.latestLog.offset))
            .orElse(partitionState.messages.lastOption.map(m => new OffsetAndMetadata(m.latestLog.offset)))
          commitOffset.map { offset =>
            (totalState + (partition -> PartitionState(prunedMessages, deleted = newDeletionMarker)), offsets + (partition -> offset))
          }.getOrElse {
            // We have an empty partition state and can't determine a valid partition to commit
            (totalState + (partition -> PartitionState(prunedMessages, deleted = newDeletionMarker)), offsets)
          }
        }
      }
    }

  private def updateState(state: Map[TopicPartition, PartitionState], records: Iterator[BytesRecord]): Map[TopicPartition, PartitionState] =
    records.foldLeft(state) {
      case (st, record) => deserializeLogMessage(record) match {
        case Failure(_) => st // TODO: handle
        case Success(LogPending(crd, timeout, epoch)) =>
          st.updated(
            crd.topicPartition,
            st.get(crd.topicPartition)
              .map(insertMessage(Message(MessageState.Pending(timeout, None, epoch), dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
              .getOrElse(
                PartitionState(
                  Vector(Message(MessageState.Pending(timeout, None, epoch), dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))),
                  deleted = -1
                )
              )
          )
        case Success(LogAcknowledged(crd)) =>
          st.updated(
            crd.topicPartition,
            st.get(crd.topicPartition)
              .map(insertMessage(Message(MessageState.Acknowledged, dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
              .getOrElse(
                PartitionState(
                  Vector(Message(MessageState.Acknowledged, dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))),
                  deleted = -1
                )
              )
          )
        case Success(LogCommitted(crd)) =>
          st.updated(
            crd.topicPartition,
            st.get(crd.topicPartition)
              .map(insertMessage(Message(MessageState.Committed, dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))))
              .getOrElse(
                PartitionState(
                  Vector(Message(MessageState.Committed, dataOffset = crd.offset, latestLog = Coord.fromConsumerRecord(record))),
                  deleted = -1
                )
              )
          )
      }
    }

  private def insertRecords(state: Map[TopicPartition, PartitionState], records: Iterator[BytesRecord]): Map[TopicPartition, PartitionState] = ???

  def iterate(
    consumer: BytesConsumer,
    state: Map[TopicPartition, PartitionState],
    stateRef: AtomicReference[Map[TopicPartition, PartitionState]]
  )(implicit ec: ExecutionContext): Future[Unit] = Future {
    val records = consumer.poll(PollTimeout)
    val (logRecords, dataRecords) = records.iterator().asScala.partition(p => LogTopics(p.topic()))
    val updated = updateState(state, logRecords)
    val withRecords = insertRecords(updated, dataRecords)

    // Try to prune state and commit
    val (pruned, commits) = prune(withRecords)

    // Update reference to state so that server can merge from it
    stateRef.set(pruned)

    if (commits.nonEmpty)
      consumer.commitSync(commits.asJava)

    // Pause and resume partitions
    val availableByPartition: Map[TopicPartition, Int] = ???
    consumer.pause(availableByPartition.filter(_._2 > PerPartitionAvailableTarget).keys.toSeq.asJava)
    consumer.resume(availableByPartition.filter(_._2 <= PerPartitionAvailableMinimum).keys.toSeq.asJava)

    pruned
  }.flatMap(iterate(consumer, _, stateRef))
}
