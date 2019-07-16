package com.mkurth.kafka.adapter

import java.nio.file.Paths
import java.time.OffsetDateTime

import com.mkurth.kafka.BuildInfo
import com.mkurth.kafka.domain.ArgumentParser
import scopt.{OParser, Read}

import scala.io.Source
import scala.util.Try

object CommandLineParser extends ArgumentParser {

  implicit private val readOffsetDateTime: Read[OffsetDateTime] = new Read[OffsetDateTime] {
    override def arity: Int = 1

    override def reads: String => OffsetDateTime = OffsetDateTime.parse
  }

  private val builder = OParser.builder[Config]
  import builder._
  private val parser = OParser.sequence(
    programName("kafkacat-avro"),
    head(s"${BuildInfo.toString}"),
    version('v', "version").text("outputs the version"),
    help('h', "help").text("prints this help message"),
    opt[String]('t', "topic").required().action((s, conf) => conf.copy(topic = s)).valueName("<topic>").text("topic to cat in kafka"),
    opt[String]('s', "schema")
      .required()
      .action((f, conf) => conf.copy(avroSchemaFile = f))
      .valueName("<avro schema file>")
      .text("json file containing the avro schema used to deserialize the message")
      .validate(fileName => Try(Source.fromFile(Paths.get(fileName).toString)).toEither.left.map(_.getMessage).map(_ => ())),
    opt[String]('c', "client-id")
      .optional()
      .action((s, conf) => conf.copy(clientId = s))
      .valueName("<client-id>")
      .text("optional client id to connect to kafka"),
    opt[String]('g', "group-id")
      .optional()
      .action((s, conf) => conf.copy(groupId = s))
      .valueName("<group-id>")
      .text("optional group id to use when connecting to kafka"),
    opt[String]('b', "bootstrap-servers")
      .required()
      .action((s, conf) => conf.copy(bootstrapServers = conf.bootstrapServers ++ s.split(",")))
      .valueName("<bootstrap-server>")
      .unbounded()
      .text("kafka bootstrap servers. coma separated or by repeating this argument."),
    opt[OffsetDateTime]('f', "from-date")
      .optional()
      .action((d, conf) => conf.copy(fromDate = d))
      .valueName("<from-date>")
      .text("OffsetDateTime from when to start parsing. format is: 2007-12-03T10:15:30+01:00"),
    opt[Option[OffsetDateTime]]('u', "until-date")
      .optional()
      .action((d, conf) => conf.copy(untilDate = d))
      .valueName("<until-date>")
      .text("Optional OffsetDateTime until (inclusive) when to scan messages. format is: 2007-12-03T10:15:30+01:00"),
    opt[Long]('l', "limit-messages")
      .optional()
      .action((l, conf) => conf.copy(messageLimit = l))
      .valueName("<limit>")
      .text("limit number of messages to read"),
  )

  def parse(args: List[String]): Option[Config] =
    OParser.parse(parser, args, Config())

}
