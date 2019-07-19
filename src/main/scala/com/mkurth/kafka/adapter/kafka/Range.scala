package com.mkurth.kafka.adapter.kafka

case class Range(start: Long, end: Long) {
  def distance: Long = end - start
}
