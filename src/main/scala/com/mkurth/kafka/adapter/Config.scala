package com.mkurth.kafka.adapter

import java.time.{Instant, OffsetDateTime, ZoneId, ZonedDateTime}

case class Config(
    topic: String                  = "needed",
    avroSchemaFile: String         = "needed",
    clientId: String               = "my console client",
    groupId: String                = "my console group",
    bootstrapServers: List[String] = List("localhost:9092"),
    fromDate: OffsetDateTime       = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()).toOffsetDateTime,
    messageLimit: Long             = Long.MaxValue
)
