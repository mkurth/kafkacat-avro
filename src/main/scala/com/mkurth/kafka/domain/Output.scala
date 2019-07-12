package com.mkurth.kafka.domain

trait Output {
  def error(msg: String): Unit
  def println(msg: String): Unit
}
