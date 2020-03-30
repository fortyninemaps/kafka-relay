package com.fortyninemaps.relay

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}

object RelayCommand {
  val port = Opts.option[Int]("port", "port to serve over")
  val topics = Opts.options[String]("topic", "Input topic to consume from (may be repeated)")
    .map(_.toList.toSet)
}

object RelayApp extends CommandApp(
  name = "kafka-relay-srv",
  header = "Start Kafka Relay server",
  main = (RelayCommand.port, RelayCommand.topics).mapN(Relay.run)
)
