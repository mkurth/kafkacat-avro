package com.mkurth.kafka.domain

trait MessageConsumer[Key, Value] {
  def read(config: Config, process: (Key, Value) => Unit)
}
