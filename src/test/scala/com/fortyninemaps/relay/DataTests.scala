package com.fortyninemaps.relay

import java.time.Instant

import com.fortyninemaps.relay.Data.{Coord, Message, MessageState, PartitionState}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.FunSpec
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.{Arbitrary, Gen}

import scala.reflect.ClassTag

// Common tests applied to all CRDTs
class CRDTLaws[T: ClassTag](name: String, merge: (T, T) => T)(implicit arb: Arbitrary[T])
  extends FunSpec with GeneratorDrivenPropertyChecks {

  implicit override val generatorDrivenConfig = PropertyCheckConfig(minSuccessful = 200)

  describe(s"the $name CRDT") {
    it("is commutative") {
      forAll { (s1: T, s2: T) =>
        val m1 = merge(s1, s2)
        val m2 = merge(s2, s1)
        assert(m1 == m2)
      }
    }

    it("is associative") {
      forAll { (s1: T, s2: T, s3: T) =>
        val m1 = merge(merge(s1, s2), s3)
        val m2 = merge(s1, merge(s2, s3))
        assert(m1 == m2)
      }
    }

    it("is idempotent") {
      forAll { (s1: T, s2: T) =>
        val m1 = merge(s1, s2)
        val m2 = merge(merge(s1, s2), s2)
        assert(m1 == m2)
      }
    }
  }
}

object ArbitraryInstances {
  val consumerRecordGen: Gen[ConsumerRecord[_, _]] = Gen.const(
    new ConsumerRecord[Array[Byte], Array[Byte]](
      "",
      0,
      0L,
      Array.empty,
      Array.empty
    )
  )

  val pendingStateGen: Gen[MessageState.Pending] =
    for {
      millis <- Gen.posNum[Long]
      instant = Instant.ofEpochMilli(millis)
      data <- Gen.option[ConsumerRecord[_, _]](consumerRecordGen)
      epoch <- Gen.choose[Int](0, 100)
    } yield MessageState.Pending(instant, data, epoch)

  val messageStateGen: Gen[MessageState] = Gen.oneOf(
    Gen.const(MessageState.Acknowledged),
    Gen.const(MessageState.Committed),
    pendingStateGen
  )

  val coordGen: Gen[Coord] = Gen.posNum[Long].map(Coord("", 0, _))

  val messageGen: Gen[Message] =
    for {
      state <- messageStateGen
      dataOffset <- Gen.posNum[Long]
      latestLog <- coordGen
    } yield Message(state, dataOffset, latestLog)

  val messageVectorGen: Gen[Vector[Message]] =
    for {
      start <- Gen.choose[Long](0, 50)
      end <- Gen.choose[Long](start, 100)
      raw <- Gen.containerOf[Vector, Message](messageGen)
    } yield (start to end zip raw).map { case (i, msg) =>
      msg.copy(dataOffset = i)
    }.toVector

  val partitionStateGen: Gen[PartitionState] =
    for {
      messages <- messageVectorGen
      minOffset = messages.map(_.dataOffset).minOption.getOrElse(-1L)
      deleted <- Gen.choose[Long](minOffset - 10, minOffset - 1)
    } yield PartitionState(messages, deleted)

  val partitionGen: Gen[TopicPartition] =
    for {
      name <- Gen.alphaStr
      partition <- Gen.choose[Int](0, 16)
    } yield new TopicPartition(name, partition)

  val stateGen: Gen[Map[TopicPartition, PartitionState]] = Gen.mapOf(
    partitionGen.flatMap { partition =>
      partitionStateGen.map { state =>
        (partition, state)
      }
    }
  )

  implicit val forMessageState: Arbitrary[MessageState] = Arbitrary(messageStateGen)
  implicit val forCoord: Arbitrary[Coord] = Arbitrary(coordGen)
  implicit val forMessageVector: Arbitrary[Vector[Message]] = Arbitrary(messageVectorGen)
  implicit val forPartitionState: Arbitrary[PartitionState] = Arbitrary(partitionStateGen)
  implicit val forState: Arbitrary[Map[TopicPartition, PartitionState]] = Arbitrary(stateGen)
}

import ArbitraryInstances._

class CoordCRDTLaws extends CRDTLaws[Coord]("Coord", Coord.merge)
class MessageStateCRDTLaws extends CRDTLaws[MessageState]("MessageState", MessageState.merge)
class MessageVectorCRDTLaws extends CRDTLaws[Vector[Message]]("MessageVector", PartitionState.mergeMessages)
class PartitionStateCRDTLaws extends CRDTLaws[PartitionState]("PartitionState", PartitionState.merge)
class StateCRDTLaws extends CRDTLaws[Map[TopicPartition, PartitionState]]("State", Data.merge)
