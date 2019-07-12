package com.mkurth.kafka

import java.nio.file.Paths

import com.mkurth.kafka.adapter.{AvroDecoder, CommandLineParser, KafkaMessageConsumer, SystemOutput}
import com.mkurth.kafka.domain.Service
import org.apache.avro.Schema

import scala.io.Source

object KafkaCat extends App {

  CommandLineParser.parse(args.toList) match {
    case Some(config) =>
      val avroSchemaFile = Source.fromFile(Paths.get(config.avroSchemaFile).toString)
      val schema         = new Schema.Parser().parse(avroSchemaFile.getLines().mkString("\n"))
      val decoder        = new AvroDecoder(schema, config)

      val service = new Service[Array[Byte], Array[Byte]](
        messageConsumer = new KafkaMessageConsumer(config),
        output          = SystemOutput,
        decoder         = decoder
      )
      service.run()
    case _ => SystemOutput.error("Invalid Config")
  }

}
