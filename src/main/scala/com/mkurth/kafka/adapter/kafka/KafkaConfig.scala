package com.mkurth.kafka.adapter.kafka

import java.time.OffsetDateTime

import com.mkurth.kafka.adapter.Config

case class KafkaConfig(clientId: String,
                       groupId: String,
                       bootstrapServers: List[String],
                       topic: String,
                       fromDate: OffsetDateTime,
                       untilDate: Option[OffsetDateTime],
                       limit: Int)

object KafkaConfig {
  def apply(config: Config): KafkaConfig = {
    import config._
    KafkaConfig(
      clientId         = clientId,
      groupId          = groupId,
      bootstrapServers = bootstrapServers,
      topic            = topic,
      fromDate         = fromDate,
      untilDate        = untilDate,
      limit            = messageLimit
    )
  }
}
