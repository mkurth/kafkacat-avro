package com.mkurth.kafka

import java.nio.file.Paths
import java.time.{Instant, OffsetDateTime, ZoneId, ZonedDateTime}

import org.apache.avro.Schema
import scopt.{OParser, Read}

import scala.io.Source
import scala.util.Try

case class Config(
    topic: String                  = "needed",
    avroSchemaFile: String         = "needed",
    clientId: String               = "my console client",
    groupId: String                = "my console group",
    bootstrapServers: List[String] = List("localhost:9092"),
    fromDate: OffsetDateTime       = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()).toOffsetDateTime,
    messageLimit: Long             = Long.MaxValue
)

object KafkaCat extends App {

  implicit private val readOffsetDateTime: Read[OffsetDateTime] = new Read[OffsetDateTime] {
    override def arity: Int = 1

    override def reads: String => OffsetDateTime = OffsetDateTime.parse
  }

  val builder = OParser.builder[Config]
  import builder._
  val parser = OParser.sequence(
    programName("kafkacat-avro"),
    head("v0.0.1"),
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
    opt[OffsetDateTime]('d', "from-date")
      .optional()
      .action((d, conf) => conf.copy(fromDate = d))
      .valueName("<from-date>")
      .text("ZonedDateTime from when to start parsing. format is: 2007-12-03T10:15:30+01:00"),
    opt[Long]('l', "limit-messages")
      .optional()
      .action((l, conf) => conf.copy(messageLimit = l))
      .valueName("<limit>")
      .text("limit number of messages to read")
  )

  OParser
    .parse(parser, args, Config())
    .foreach(config => {
      val avroSchemaFile = Source.fromFile(Paths.get(config.avroSchemaFile).toString)
      val schema         = new Schema.Parser().parse(avroSchemaFile.getLines().mkString("\n"))

      KafkaMessageConsumer.read(KafkaConfig(config), (key, value) => {
        val decoded = AvroDecoder.deserialize(value, schema)
        println(decoded)
      })
    })

}
