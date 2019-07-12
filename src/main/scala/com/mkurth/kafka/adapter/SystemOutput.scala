package com.mkurth.kafka.adapter

import com.mkurth.kafka.domain.Output

object SystemOutput extends Output {
  def error(msg: String): Unit   = System.err.println(msg)
  def println(msg: String): Unit = System.out.println(msg)
}
