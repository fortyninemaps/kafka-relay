package com.fortyninemaps.relay

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import com.fortyninemaps.relay.Data.{Coord, Message, MessageState, PartitionState}
import com.fortyninemaps.relay.LogData.{LogAcknowledged, LogCommitted, LogMessage, LogPending}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// The consumer reads from both data and log topics, updating a copy of the state-map
object Consumer {
  type BytesConsumer = KafkaConsumer[Array[Byte], Array[Byte]]
  type BytesRecord = ConsumerRecord[Array[Byte], Array[Byte]]

  val PollTimeout: java.time.Duration = java.time.Duration.ofMillis(500)
  val PerPartitionAvailableTarget: Int = 1000
  val PerPartitionAvailableMinimum: Int = 500
  val LogTopics: Set[String] = Set("relay.log")

  private def deserializeLogMessage(rec: ConsumerRecord[Array[Byte], Array[Byte]]): Try[LogMessage] = ???

  // Insert a message into the internal state vector, potentially overwriting a previous version
  private[relay] def insertMessage(message: Message)(partitionState: PartitionState): PartitionState =
    PartitionState.merge(partitionState, PartitionState.of(message))

  // Trim all committed messages from the internal state vector while committing to Kafka
  private def prune(state: Map[TopicPartition, PartitionState]): (Map[TopicPartition, PartitionState], Map[TopicPartition, OffsetAndMetadata]) =
    state.foldLeft((Map.empty[TopicPartition, PartitionState], Map.empty[TopicPartition, OffsetAndMetadata])) {
      case ((totalState, offsets), (partition, partitionState)) =>
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

  // Given an iterator of log messages, update the state map with messages that have been sent, acknowledged, and
  // committed.
  // FIXME: why do we need to log commits, since they happen in this loop?
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

  // Given an iterator of messages, update the state map by inserting them as ready to send
  // Nota bene: a new message is represented as having a Bottom timeout and a zero epoch
  @tailrec
  private def insertRecords(state: Map[TopicPartition, PartitionState], records: List[BytesRecord]): Map[TopicPartition, PartitionState] =
    records match {
      case Nil => state
      case hd :: tail =>
        val tp = new TopicPartition(hd.topic(), hd.partition())
        val message = Message(
          state = MessageState.Pending(Instant.MIN, Some(hd), 0),
          dataOffset = hd.offset(),
          latestLog = Coord("", 0, 0L) // FIXME: hack because there is no log
        )
        val prev = state.getOrElse(tp, PartitionState.empty)
        insertRecords(
          state = state.updated(key = tp, value = insertMessage(message)(prev)),
          tail
        )
    }

  def iterate(
    consumer: BytesConsumer,
    state: Map[TopicPartition, PartitionState],
    stateRef: AtomicReference[Map[TopicPartition, PartitionState]]
  )(implicit ec: ExecutionContext): Future[Unit] = Future {
    val records = consumer.poll(PollTimeout)
    val (logRecords, dataRecords) = records.iterator().asScala.partition(p => LogTopics(p.topic()))
    val mkNewState = { updateState(_, logRecords) } andThen { insertRecords(_, dataRecords.toList) }
    val updatedState = mkNewState(state)

    // Decide what can be committed, and the state we'd have if we did so
    val (prunedState, commits) = prune(updatedState)

    // Try to commit, but if we fail, stick with the non-pruned state
    val newState = if (commits.nonEmpty)
      Try(consumer.commitSync(commits.asJava)).map(_ => prunedState).getOrElse(updatedState)
    else prunedState

    // Update reference to state so that server can merge from it
    stateRef.set(newState)

    // Pause and resume partitions based on the number of messages in each data partition that are available to send
    val now = Instant.now
    val availableByPartition: Map[TopicPartition, Int] =
      newState.map { case (topicPartition: TopicPartition, partitionState: PartitionState) =>
        topicPartition -> partitionState.messages.map(_.state).count {
          case MessageState.Pending(timeout, _, _) if timeout.isBefore(now) => true
          case _ => false
        }
      }
    consumer.pause(availableByPartition.filter(_._2 > PerPartitionAvailableTarget).keys.toSeq.asJava)
    consumer.resume(availableByPartition.filter(_._2 <= PerPartitionAvailableMinimum).keys.toSeq.asJava)

    newState
  }.flatMap(iterate(consumer, _, stateRef))
}
