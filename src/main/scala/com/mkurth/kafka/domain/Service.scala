package com.mkurth.kafka.domain

class Service[K, V](messageConsumer: MessageConsumer[K, V], output: Output, decoder: Decoder[V]) {

  def run(): Unit =
    messageConsumer.read(process = (_, value) => output.println(decoder.decode(value)))

}
