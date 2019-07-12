package com.mkurth.kafka.domain

class Service[K, V](messageConsumer: MessageConsumer[K, V], output: Output, decoder: Decoder[V]) {

  def run(config: Config): Unit =
    messageConsumer.read(config, process = (_, value) => output.println(decoder.decode(value)))

}
