package com.mkurth.kafka.domain

trait MessageConsumer[Key, Value] {
  def read(process: (Key, Value) => Unit)
}
