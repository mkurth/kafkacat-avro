package com.mkurth.kafka.adapter

import java.lang
import java.time.Duration
import java.util.Properties

import com.mkurth.kafka.domain.MessageConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
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
    val offsetRanges   = calculateOffsetRanges(consumerOffset, latestOffsets)

    if (consumerOffset.isEmpty)
      logger.error("no offsets found for given date. try an earlier date.")
    else {
      initKafkaClient(kafkaConfig, consumer)
      seekToOffset(consumer, offsetRanges, kafkaConfig.topic)
      val assignments = consumer.assignment().asScala.toList.map(_.partition())
      run(consumer, kafkaConfig, offsetRanges, process, 0, assignments)
      consumer.close()
    }
  }

  @tailrec
  private def run(consumer: KafkaConsumer[Key, Value],
                  kafkaConfig: KafkaConfig,
                  offsetRanges: Map[Int, Range],
                  process: (Key, Value) => Unit,
                  readSoFar: Int,
                  assignments: List[Int]): Int =
    if (!(limitNotReached(kafkaConfig, readSoFar) && assignments.nonEmpty)) {
      readSoFar
    } else {
      val processed = consumer
        .poll(defaultTimeout)
        .iterator()
        .asScala
        .toList
        .map(record => {
          val assign = pausePartitionsThatReachedTheEnd(offsetRanges, consumer, record, kafkaConfig.topic, assignments)
          if (readSoFar < kafkaConfig.limit && recordInRange(offsetRanges, record)) {
            process(record.key(), record.value())
            assign -> 1
          } else assign -> 0
        })
      consumer.commitSync()
      run(consumer, kafkaConfig, offsetRanges, process, readSoFar + processed.map(_._2).sum, processed.last._1)
    }

  private def initKafkaClient(kafkaConfig: KafkaConfig, consumer: KafkaConsumer[Key, Value]) = {
    consumer.subscribe(List(kafkaConfig.topic).asJava)
    Try(consumer.poll(Duration.ofSeconds(5)))
  }

  private def calculateOffsetRanges(consumerOffset: Map[Int, Long], latestOffsets: Option[Map[Int, Long]]): Map[Int, Range] =
    latestOffsets
      .map(off =>
        off.map {
          case (partition, offset) => (partition, Range(consumerOffset.getOrElse(partition, Long.MaxValue), offset))
      })
      .getOrElse(consumerOffset.map {
        case (partition, offset) => (partition, Range(offset, Long.MaxValue))
      })
      .toMap

  private def recordInRange(offsetRanges: Map[Int, Range], record: ConsumerRecord[Key, Value]) =
    offsetRanges.get(record.partition()).exists(_.end >= record.offset())

  private def limitNotReached(kafkaConfig: KafkaConfig, readSoFar: Int) =
    readSoFar < kafkaConfig.limit

  private def pausePartitionsThatReachedTheEnd(offsetRanges: Map[Int, Range],
                                               consumer: KafkaConsumer[Key, Value],
                                               record: ConsumerRecord[Key, Value],
                                               topic: String,
                                               assignments: List[Int]) =
    offsetRanges
      .find(lastAllowedOffset => record.partition() == lastAllowedOffset._1 && record.offset() >= lastAllowedOffset._2.end)
      .map(offset => {
        Try(consumer.pause(List(new TopicPartition(topic, offset._1)).asJava))
        assignments.filter(_ != offset._1)
      })
      .getOrElse(assignments)

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

case class Range(start: Long, end: Long) {
  def distance: Long = end - start
}
