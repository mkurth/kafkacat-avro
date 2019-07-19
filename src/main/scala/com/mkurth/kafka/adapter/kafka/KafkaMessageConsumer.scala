package com.mkurth.kafka.adapter.kafka

import java.lang
import java.time.Duration
import java.util.Properties

import com.mkurth.kafka.adapter.Config
import com.mkurth.kafka.adapter.kafka.KafkaMessageConsumer.readFromKafka
import com.mkurth.kafka.domain.MessageConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Try

class KafkaMessageConsumer(baseConfig: Config) extends MessageConsumer[Array[Byte], Array[Byte]] {

  type Key   = Array[Byte]
  type Value = Array[Byte]

  private val logger         = LoggerFactory.getLogger("")
  private val defaultTimeout = Duration.ofMinutes(5)
  private val kafkaConfig    = KafkaConfig(baseConfig)
  private val config         = createConfig(kafkaConfig)
  private val consumer       = new KafkaConsumer[Key, Value](config)
  private val partitions     = consumer.partitionsFor(kafkaConfig.topic).asScala.toList
  private val dateOffset     = new java.lang.Long(kafkaConfig.fromDate.toInstant.toEpochMilli)

  def read(process: (Key, Value) => Unit): Unit = {
    val consumerOffset = findOffsetsForDate(consumer, partitions, dateOffset)
    val latestOffsets  = kafkaConfig.untilDate.map(odt => findOffsetsForDate(consumer, partitions, new lang.Long(odt.toInstant.toEpochMilli)))
    val offsetRanges   = OffsetCalculator.calculateOffsetRanges(consumerOffset, latestOffsets)

    if (consumerOffset.isEmpty)
      logger.error("no offsets found for given date. try an earlier date.")
    else {
      initKafkaClient(kafkaConfig, consumer)
      seekToOffset(consumer, offsetRanges, kafkaConfig.topic)
      val assignments = consumer.assignment().asScala.toList.map(_.partition())
      readFromKafka(
        poll             = () => consumer.poll(defaultTimeout),
        commit           = () => consumer.commitSync(),
        pause            = (partition: Int) => consumer.pause(List(new TopicPartition(kafkaConfig.topic, partition)).asJava),
        kafkaConfig      = kafkaConfig,
        offsetRanges     = offsetRanges,
        process          = process,
        readSoFar        = 0,
        activePartitions = assignments
      )
      consumer.close()
    }
  }

  private def initKafkaClient(kafkaConfig: KafkaConfig, consumer: KafkaConsumer[Key, Value]) = {
    consumer.subscribe(List(kafkaConfig.topic).asJava)
    Try(consumer.poll(Duration.ofSeconds(5)))
  }

  private def seekToOffset(consumer: KafkaConsumer[Key, Value], consumerOffset: Map[Int, Range], topic: String): Unit =
    consumerOffset
      .filter(_._2.distance != 0)
      .foreach({
        case (partition, offset) =>
          consumer.seek(new TopicPartition(topic, partition), offset.start)
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

  private def findOffsetsForDate(consumer: KafkaConsumer[Key, Value], partitions: List[PartitionInfo], dateOffset: lang.Long): Map[Int, Long] =
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
      .toList
      .filter(_._2 != null)
      .map(tp => tp._1.partition() -> tp._2.offset())
      .toMap
}

object KafkaMessageConsumer {

  @tailrec
  final def readFromKafka[Key, Value](poll: () => ConsumerRecords[Key, Value],
                                      commit: () => Unit,
                                      pause: Int => Unit,
                                      kafkaConfig: KafkaConfig,
                                      offsetRanges: Map[Int, Range],
                                      process: (Key, Value) => Unit,
                                      readSoFar: Int,
                                      activePartitions: List[Int]): Int =
    if (!(readSoFar < kafkaConfig.limit && activePartitions.nonEmpty)) {
      readSoFar
    } else {
      val processed = poll()
        .iterator()
        .asScala
        .toList
        .map(record => {
          val assign = pausePartitionsThatReachedTheEnd(offsetRanges, pause, record, kafkaConfig.topic, activePartitions)
          if (readSoFar < kafkaConfig.limit && OffsetCalculator.recordInRange(offsetRanges, record)) {
            process(record.key(), record.value())
            assign -> 1
          } else assign -> 0
        })
      commit()
      readFromKafka(poll,
                    commit,
                    pause,
                    kafkaConfig,
                    offsetRanges,
                    process,
                    readSoFar + processed.map(_._2).sum,
                    Try(processed.last._1).getOrElse(List.empty))
    }

  private def pausePartitionsThatReachedTheEnd[Key, Value](offsetRanges: Map[Int, Range],
                                                           pause: Int => Unit,
                                                           record: ConsumerRecord[Key, Value],
                                                           topic: String,
                                                           assignments: List[Int]) =
    offsetRanges
      .find(lastAllowedOffset => record.partition() == lastAllowedOffset._1 && record.offset() >= lastAllowedOffset._2.end)
      .map(offset => {
        Try(pause(offset._1))
        assignments.filter(_ != offset._1)
      })
      .getOrElse(assignments)
}
