package com.fortyninemaps.relay

import java.time.Instant

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec

// data types associated with the internal state vector
// defines merge operations for the state CRDT
object Data {
  case class Coord(topic: String, partition: Int, offset: Long) {
    lazy val topicPartition = new TopicPartition(topic, partition)
    lazy val offsetMap = Map(topicPartition -> new OffsetAndMetadata(offset))
  }
  object Coord {
    def fromConsumerRecord(rec: ConsumerRecord[_, _]): Coord = Coord(rec.topic(), rec.partition(), rec.offset())

    def merge(left: Coord, right: Coord): Coord =
      if (left.offset < right.offset) right else left
  }

  def mergeOpt[T](left: Option[T], right: Option[T]): Option[T] =
    (left, right) match {
      case (Some(_), _) => left
      case _ => right
    }

  // Represents the state of a Kafka message in the internal state vector
  sealed trait MessageState
  object MessageState {
    case class Pending(timeout: Instant, data: Option[ConsumerRecord[_, _]], epoch: Int) extends MessageState
    case object Acknowledged extends MessageState
    case object Committed extends MessageState

    def mergeInstant(left: Instant, right: Instant): Instant =
      if (left.isBefore(right)) right
      else left

    // Acknowledged follows Pending; Committed follows Acknowledged
    // When comparing pending, take the one with the latest epoch, merging the data.
    // If the epochs are the same, chose the one with the latest timeout.
    def merge(left: MessageState, right: MessageState): MessageState =
      (left, right) match {
        case (Pending(_, d1, e1), Pending(t2, d2, e2)) if e1 < e2 =>  Pending(t2, mergeOpt(d1, d2), e2)
        case (Pending(t1, d1, e1), Pending(_, d2, e2)) if e1 > e2 =>  Pending(t1, mergeOpt(d1, d2), e1)
        case (Pending(t1, d1, e1), Pending(t2, d2, e2)) if e1 == e2 =>  Pending(mergeInstant(t1, t2), mergeOpt(d1, d2), e1)
        case (Pending(_, _, _), Acknowledged) => Acknowledged
        case (Acknowledged, Pending(_, _, _)) => Acknowledged
        case (Acknowledged, Acknowledged) => Acknowledged
        case (_, Committed) => Committed
        case (Committed, _) => Committed
      }
  }

  case class Message(state: MessageState, dataOffset: Long, latestLog: Coord)
  case object Message {
    def mergeOption(left: Message, right: Message): Option[Message] =
      if (left.dataOffset != right.dataOffset) None
      else Some(
        Message(
          state = MessageState.merge(left.state, right.state),
          dataOffset = left.dataOffset,
          latestLog = Coord.merge(left.latestLog, right.latestLog)
        )
      )
  }

  // Sequence of messages ordered by data offset. Messages with offsets <= deleted have been pruned
  case class PartitionState(messages: Vector[Message], deleted: Long)
  object PartitionState {
    val empty: PartitionState = PartitionState(Vector.empty, 0L)

    def of(message: Message): PartitionState = PartitionState(Vector(message), 0L)

    @tailrec
    def mergeMessages(left: Vector[Message], right: Vector[Message]): Vector[Message] =
      if (left.isEmpty) right
      else if (right.isEmpty) left
      else {
        val l1 = left.head.dataOffset
        val l2 = left.last.dataOffset
        val r1 = right.head.dataOffset
        val r2 = right.last.dataOffset

        if (l2 < r1) {
          // non-intersecting with left first; fill in between with Pending
          val fill = Vector.range(l2 + 1, r1).map { offset =>
            Message(MessageState.Pending(Instant.MIN, None, -1), offset, Coord("", 0, -1))
          }
          left ++ fill ++ right
        } else if (l1 <= r1 && l2 <= r2) {
          // intersecting with left first
          val merged = (left.drop((r1 - l1).toInt) zip right.take((l2 - r1).toInt + 1)).map {
            // If this returns None, our segments weren't aligned
              case (l, r) => Message.mergeOption(l, r).get
            }
          left.take((r1 - l1).toInt) ++ merged ++ right.drop((l2 - r1).toInt + 1)
        } else if (l1 <= r1 && l2 >= r2) {
          // intersecting with left spanning right
          val merged = (left.slice((r1 - l1).toInt, (r2 - l1).toInt + 1) zip right).map {
            // If this returns None, our segments weren't aligned
            case (l, r) => Message.mergeOption(l, r).get
          }
          left.take((r1 - l1).toInt) ++ merged ++ left.drop((r2 - l1).toInt + 1)
        } else mergeMessages(right, left)
      }

    def merge(left: PartitionState, right: PartitionState): PartitionState = {
      val deleted = left.deleted max right.deleted
      val messages = mergeMessages(left.messages, right.messages).filter(_.dataOffset > deleted)
      PartitionState(messages = messages, deleted = deleted)
    }
  }

  def merge(left: Map[TopicPartition, PartitionState], right: Map[TopicPartition, PartitionState]): Map[TopicPartition, PartitionState] =
    (left.keySet ++ right.keySet).map { partition =>
      partition -> (
        if (!left.contains(partition)) right(partition)
        else if (!right.contains(partition)) left(partition)
        else PartitionState.merge(left(partition), right(partition))
      )
    }.toMap
}
