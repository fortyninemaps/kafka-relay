package com.fortyninemaps.relay

import java.time.Instant

import com.fortyninemaps.relay.Data.Coord
import com.fortyninemaps.relay.LogData.{LogAcknowledged, LogCommitted, LogMessage, LogPending}
import io.circe.syntax._
import io.circe.parser.decode
import org.scalatest.FunSpec
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.{Arbitrary, Gen}

class SerializationTests extends FunSpec with GeneratorDrivenPropertyChecks {

  val coordGen: Gen[Coord] = for {
    topic <- Gen.alphaStr
    partition <- Gen.choose(0, 32)
    offset <- Gen.posNum[Long]
  } yield Coord(topic, partition, offset)

  val logMessageGen: Gen[LogMessage] = Gen.choose(0, 3).flatMap {
    case 0 => for {
      coord <- coordGen
      instant <- Gen.posNum[Long].map(Instant.ofEpochMilli)
      epoch <- Gen.posNum[Int]
    } yield LogPending(coord, instant, epoch)
    case 1 => coordGen.map(LogAcknowledged)
    case 2 => coordGen.map(LogCommitted)
    case other => throw new RuntimeException(s"no ADT variant: $other")
  }

  implicit val forCoord: Arbitrary[Coord] = Arbitrary(coordGen)
  implicit val forLogMessage: Arbitrary[LogMessage] = Arbitrary(logMessageGen)

  describe("JSON codec for Coord") {
    it("roudtrips") {
      forAll { (coord: Coord) =>
        assert(decode[Coord](coord.asJson.noSpaces) == Right(coord))
      }
    }
  }

  describe("JSON codec for LogMessage") {
    it("roudtrips") {
      forAll { (logMessage: LogMessage) =>
        assert(decode[LogMessage](logMessage.asJson.noSpaces) == Right(logMessage))
      }
    }
  }
}
