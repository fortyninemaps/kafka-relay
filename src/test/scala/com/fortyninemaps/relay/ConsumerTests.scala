package com.fortyninemaps.relay

import java.time.Instant

import com.fortyninemaps.relay.Data.{Coord, Message, MessageState, PartitionState}
import org.scalatest.FunSpec

class ConsumerTests extends FunSpec {
  describe("when inserting a record") {
    val partitionState = PartitionState(
      messages = Vector(
        Message(MessageState.Committed, 4, Coord("log", 0, 3L)),
        Message(MessageState.Acknowledged, 5, Coord("log", 0, 4L)),
        Message(MessageState.Acknowledged, 6, Coord("log", 0, 5L)),
        Message(MessageState.Pending(Instant.ofEpochMilli(100), None, 1), 7, Coord("log", 0, 6L)),
        Message(MessageState.Pending(Instant.MIN, None, 0), 8, Coord("log", 0, 7L)),
        Message(MessageState.Pending(Instant.MIN, None, 0), 9, Coord("log", 0, 8L)),
      ),
      deleted = 2L
    )

    it("the result contains the record inserted") {
      val message = Message(MessageState.Pending(Instant.MIN, None, 0), 10, Coord("log", 0, 9L))
      val result = Consumer.insertMessage(message)(partitionState)
      assert(result.messages.contains(message))
      assert(result.messages.size == partitionState.messages.size + 1)
    }

    it("can insert into an empty partition") {
      val message = Message(MessageState.Pending(Instant.MIN, None, 0), 10, Coord("log", 0, 9L))
      val result = Consumer.insertMessage(message)(PartitionState.empty)
      assert(result.messages.contains(message))
      assert(result.messages.size == 1)
    }

    it("overwrites an older record") {
      val message = Message(MessageState.Pending(Instant.ofEpochMilli(200), None, 1), 8, Coord("log", 0, 9L))
      val result = Consumer.insertMessage(message)(partitionState)
      assert(result.messages.contains(message))
      assert(result.messages.size == partitionState.messages.size)
    }

    it("retains a newer record") {
      val message = Message(MessageState.Pending(Instant.ofEpochMilli(200), None, 1), 6, Coord("log", 0, 9L))
      val result = Consumer.insertMessage(message)(partitionState)
      assert(!result.messages.contains(message))
      assert(result.messages.size == partitionState.messages.size)
    }
  }
}
