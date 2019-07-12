package com.mkurth.kafka.adapter

import java.time.OffsetDateTime

case class KafkaConfig(clientId: String, groupId: String, bootstrapServers: List[String], topic: String, fromDate: OffsetDateTime, limit: Long)

object KafkaConfig {
  def apply(config: Config): KafkaConfig = {
    import config._
    KafkaConfig(
      clientId         = clientId,
      groupId          = groupId,
      bootstrapServers = bootstrapServers,
      topic            = topic,
      fromDate         = fromDate,
      limit            = messageLimit
    )
  }
}
