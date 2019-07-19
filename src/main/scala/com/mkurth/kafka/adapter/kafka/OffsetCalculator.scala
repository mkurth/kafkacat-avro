package com.mkurth.kafka.adapter.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

object OffsetCalculator {

  def calculateOffsetRanges(startingOffsets: Map[Int, Long], endingOffsets: Option[Map[Int, Long]]): Map[Int, Range] =
    endingOffsets
      .map(_.map {
        case (partition, offset) => (partition, Range(startingOffsets.getOrElse(partition, Long.MaxValue), offset))
      })
      .getOrElse(startingOffsets.map {
        case (partition, offset) => (partition, Range(offset, Long.MaxValue))
      })
      .toMap

  def recordInRange[Key, Value](offsetRanges: Map[Int, Range], record: ConsumerRecord[Key, Value]): Boolean =
    offsetRanges.get(record.partition()).exists(_.end >= record.offset())

}
