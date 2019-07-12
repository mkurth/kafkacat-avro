package com.mkurth.kafka

import java.time.{Duration, OffsetDateTime}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.jdk.CollectionConverters._

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

object KafkaMessageConsumer {

  type Key   = Array[Byte]
  type Value = Array[Byte]

  def read(kafkaConfig: KafkaConfig, process: (Key, Value) => Unit): Unit = {
    val config = new Properties()
    config.put("client.id", kafkaConfig.clientId)
    config.put("group.id", kafkaConfig.groupId)
    config.put("bootstrap.servers", kafkaConfig.bootstrapServers.mkString(","))
    config.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    val consumer = new KafkaConsumer[Key, Value](config)

    val partitions = consumer.partitionsFor(kafkaConfig.topic).asScala.toList
    val dateOffset = new java.lang.Long(kafkaConfig.fromDate.toInstant.toEpochMilli)

    val consumerOffset: mutable.Map[TopicPartition, OffsetAndTimestamp] = consumer
      .offsetsForTimes(
        partitions
          .map(partitionInfo => {
            (new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), dateOffset)
          })
          .toMap
          .asJava,
        Duration.ofMinutes(5)
      )
      .asScala

    consumer.assign(consumerOffset.keys.toList.asJava)
    consumerOffset.foreach({
      case (partition, offset) =>
        println(s"$partition: ${offset.offset()}")
        consumer.seek(partition, offset.offset())
    })

    val readSoFar = new AtomicInteger()

    while (readSoFar.get() < kafkaConfig.limit) {
      val records: ConsumerRecords[Key, Value] = consumer.poll(Duration.ofMinutes(5))
      records.iterator().asScala.foreach(record => process(record.key(), record.value()))
      consumer.commitSync()
      readSoFar.set(readSoFar.get() + records.count())
    }
  }
}
