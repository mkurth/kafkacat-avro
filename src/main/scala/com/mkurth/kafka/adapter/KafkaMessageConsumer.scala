package com.mkurth.kafka.adapter

import java.lang
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import com.mkurth.kafka.domain.MessageConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

class KafkaMessageConsumer(baseConfig: Config) extends MessageConsumer[Array[Byte], Array[Byte]] {

  type Key   = Array[Byte]
  type Value = Array[Byte]
  private val logger: Logger           = LoggerFactory.getLogger("")
  private val defaultTimeout: Duration = Duration.ofMinutes(5)

  def read(process: (Key, Value) => Unit): Unit = {
    val kafkaConfig    = KafkaConfig(baseConfig)
    val config         = createConfig(kafkaConfig)
    val consumer       = new KafkaConsumer[Key, Value](config)
    val partitions     = consumer.partitionsFor(kafkaConfig.topic).asScala.toList
    val dateOffset     = new java.lang.Long(kafkaConfig.fromDate.toInstant.toEpochMilli)
    val consumerOffset = findOffsetsForDate(consumer, partitions, dateOffset)
    val latestOffsets  = kafkaConfig.untilDate.map(odt => findOffsetsForDate(consumer, partitions, new lang.Long(odt.toInstant.toEpochMilli)))

    consumer.assign(consumerOffset.keys.toList.asJava)
    val noOffsetsFound = consumerOffset.forall(_._2 == null)
    if (noOffsetsFound)
      logger.error("no offsets found for given date. try an earlier date.")
    else {
      seekToOffset(consumer, consumerOffset)
      val readSoFar = new AtomicInteger()
      while (limitNotReached(kafkaConfig, readSoFar)) {
        val records: ConsumerRecords[Key, Value] = consumer.poll(defaultTimeout)
        records
          .iterator()
          .asScala
          .foreach(record => {
            pausePartitionsThatReachedTheEnd(latestOffsets, consumer, record)
            if (readSoFar.getAndIncrement() < kafkaConfig.limit) process(record.key(), record.value())
          })
        consumer.commitSync()
      }
      consumer.close()
    }
  }

  private def limitNotReached(kafkaConfig: KafkaConfig, readSoFar: AtomicInteger) =
    readSoFar.get() < kafkaConfig.limit

  private def pausePartitionsThatReachedTheEnd(maxTimestamp: Option[mutable.Map[TopicPartition, OffsetAndTimestamp]],
                                               consumer: KafkaConsumer[Key, Value],
                                               record: ConsumerRecord[Key, Value]): Unit =
    maxTimestamp
      .flatMap(_.find(lastAllowedOffset => record.partition() == lastAllowedOffset._1.partition() && record.offset() > lastAllowedOffset._2.offset()))
      .foreach(offset => Try(consumer.pause(List(offset._1).asJava)))

  private def seekToOffset(consumer: KafkaConsumer[Key, Value], consumerOffset: mutable.Map[TopicPartition, OffsetAndTimestamp]): Unit =
    consumerOffset
      .filter(_._2 != null)
      .foreach({
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
        defaultTimeout
      )
      .asScala
}
