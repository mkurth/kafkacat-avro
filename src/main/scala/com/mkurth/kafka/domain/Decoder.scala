package com.mkurth.kafka.domain

trait Decoder[A] {
  def decode(a: A): String
}
