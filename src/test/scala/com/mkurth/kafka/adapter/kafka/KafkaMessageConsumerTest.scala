package com.mkurth.kafka.adapter.kafka

import java.time.OffsetDateTime
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FlatSpec, Matchers}

import scala.jdk.CollectionConverters._

class KafkaMessageConsumerTest extends FlatSpec with Matchers {

  private val unlimitedOffsetRange = Map(0 -> Range(0, Long.MaxValue))

  "KafkaMessageConsumer" should "process only as much messages as defined by limit" in {
    val processCount = new AtomicInteger(0)
    val processed = KafkaMessageConsumer.readFromKafka(
      poll             = createRandomRecords(10),
      commit           = () => (),
      pause            = (i: Int) => (),
      kafkaConfig      = createDummyConfig.copy(limit = 2),
      offsetRanges     = unlimitedOffsetRange,
      process          = (k: String, v: String) => processCount.incrementAndGet(),
      readSoFar        = 0,
      activePartitions = List(0)
    )

    processCount.get() shouldBe 2
    processed shouldBe 2
  }

  it should "process only messages that are in offset range" in {
    val processCount = new AtomicInteger(0)
    val processed = KafkaMessageConsumer.readFromKafka(
      poll             = createRandomRecords(10),
      commit           = () => (),
      pause            = (i: Int) => (),
      kafkaConfig      = createDummyConfig,
      offsetRanges     = Map(0 -> Range(0, 1)),
      process          = (k: String, v: String) => processCount.incrementAndGet(),
      readSoFar        = 0,
      activePartitions = List(0)
    )

    processCount.get() shouldBe 2
    processed shouldBe 2
  }

  it should "not poll anything, if there is no active partition left" in {
    val processCount = new AtomicInteger(0)
    val processed = KafkaMessageConsumer.readFromKafka(
      poll             = createRandomRecords(10),
      commit           = () => (),
      pause            = (i: Int) => (),
      kafkaConfig      = createDummyConfig,
      offsetRanges     = unlimitedOffsetRange,
      process          = (k: String, v: String) => processCount.incrementAndGet(),
      readSoFar        = 0,
      activePartitions = List()
    )

    processCount.get() shouldBe 0
    processed shouldBe 0
  }

  it should "pause a partition that reached the end of given ranges" in {
    val partition0Paused = new AtomicBoolean(false)
    val processed = KafkaMessageConsumer.readFromKafka(
      poll             = () => if (partition0Paused.get()) createRandomRecords(20, 1).apply() else createRandomRecords(20).apply(),
      commit           = () => (),
      pause            = (i: Int) => if (i == 0) partition0Paused.set(true),
      kafkaConfig      = createDummyConfig,
      offsetRanges     = Map(0 -> Range(0, 9), 1 -> Range(0, 9)),
      process          = (k: String, v: String) => (),
      readSoFar        = 0,
      activePartitions = List(0, 1)
    )

    partition0Paused.get() shouldBe true
    processed shouldBe 20
  }

  it should "commit after each poll" in {
    val commitCount = new AtomicInteger(0)
    val pollCount   = new AtomicInteger(0)
    val processed = KafkaMessageConsumer.readFromKafka(
      poll = () => {
        pollCount.incrementAndGet()
        createRandomRecords(10).apply()
      },
      commit           = () => commitCount.incrementAndGet(),
      pause            = (i: Int) => (),
      kafkaConfig      = createDummyConfig.copy(limit = 200),
      offsetRanges     = unlimitedOffsetRange,
      process          = (k: String, v: String) => (),
      readSoFar        = 0,
      activePartitions = List(0, 1)
    )

    processed shouldBe 200
    pollCount.get() shouldBe commitCount.get()
  }

  private def createRandomRecords(amount: Int, partition: Int = 0) = { () =>
    val records = (0 until amount).map(i => new ConsumerRecord("test", partition, i, s"key$i", s"value$i")).toList
    new ConsumerRecords[String, String](Map(new TopicPartition("test", partition) -> records.asJava).asJava),
  }

  private def createDummyConfig = KafkaConfig(
    clientId         = "client",
    groupId          = "group",
    bootstrapServers = List.empty,
    topic            = "test",
    fromDate         = OffsetDateTime.MIN,
    untilDate        = None,
    limit            = Int.MaxValue
  )
}
