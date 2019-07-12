package com.mkurth.kafka.domain

import com.mkurth.kafka.adapter.Config

trait ArgumentParser {
  def parse(args: List[String]): Option[Config]
}
