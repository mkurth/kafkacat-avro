package com.mkurth.kafka.adapter

import java.lang
import java.time.{Duration, OffsetDateTime}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import com.mkurth.kafka.domain.{Config, MessageConsumer}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object KafkaMessageConsumer extends MessageConsumer[Array[Byte], Array[Byte]] {

  type Key   = Array[Byte]
  type Value = Array[Byte]
  val logger: Logger = LoggerFactory.getLogger("")

  def read(baseConfig: Config, process: (Key, Value) => Unit): Unit = {
    val kafkaConfig    = KafkaConfig(baseConfig)
    val config         = createConfig(kafkaConfig)
    val consumer       = new KafkaConsumer[Key, Value](config)
    val partitions     = consumer.partitionsFor(kafkaConfig.topic).asScala.toList
    val dateOffset     = new java.lang.Long(kafkaConfig.fromDate.toInstant.toEpochMilli)
    val consumerOffset = findOffsetsForDate(consumer, partitions, dateOffset)

    consumer.assign(consumerOffset.keys.toList.asJava)
    val noOffsetsFound = consumerOffset.forall(_._2 == null)
    if (noOffsetsFound)
      logger.error("no offsets found for given date. try an earlier date.")
    else {
      seekToOffset(consumer, consumerOffset)
      val readSoFar = new AtomicInteger()
      while (readSoFar.get() < kafkaConfig.limit) {
        val records: ConsumerRecords[Key, Value] = consumer.poll(Duration.ofMinutes(5))
        records
          .iterator()
          .asScala
          .foreach(record => {
            if (readSoFar.getAndIncrement() < kafkaConfig.limit) process(record.key(), record.value())
          })
        consumer.commitSync()
      }
    }
  }

  private def seekToOffset(consumer: KafkaConsumer[Key, Value], consumerOffset: mutable.Map[TopicPartition, OffsetAndTimestamp]): Unit =
    consumerOffset.foreach({
      case (partition, offset) =>
        consumer.seek(partition, offset.offset())
    })

  private def createConfig(kafkaConfig: KafkaConfig): Properties = {
    val config = new Properties()
    config.put("client.id", kafkaConfig.clientId)
    config.put("group.id", kafkaConfig.groupId)
    config.put("bootstrap.servers", kafkaConfig.bootstrapServers.mkString(","))
    config.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    config
  }

  private def findOffsetsForDate(consumer: KafkaConsumer[Key, Value],
                                 partitions: List[PartitionInfo],
                                 dateOffset: lang.Long): mutable.Map[TopicPartition, OffsetAndTimestamp] =
    consumer
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
}
