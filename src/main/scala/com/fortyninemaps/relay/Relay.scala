package com.fortyninemaps.relay

import java.time.Instant
import java.util
import java.util.concurrent.atomic.AtomicReference

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

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

  // Handle incoming requests while writing to and reading from the log topic. We can only send out messages once we've
  // read our own write!
  private def serve(
    port: Int,
    stateRef: AtomicReference[Map[TopicPartition, PartitionState]]
  ): Nothing = ???

  def run(port: Int, inputTopics: Set[String]): Nothing = {

    val kafkaConfig: Map[String, AnyRef] = ???
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaConfig.asJava)
    consumer.subscribe((Consumer.LogTopics ++ inputTopics).asJava, new RebalanceListener)
    // TODO: pause data topics

    // The poller reads from data and log topics. It updates a reference to its internal state, which
    // the server uses to choose messages to serve.
    val ref = new AtomicReference(Map.empty[TopicPartition, PartitionState])
    Consumer.iterate(consumer, Map.empty, ref)(ExecutionContext.global)

    // The server serves messages to clients and frequently merges the poller's state into its own state.
    serve(port, ref)
  }

  class RebalanceListener[K, V] extends ConsumerRebalanceListener {
    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {}

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}
  }

}

