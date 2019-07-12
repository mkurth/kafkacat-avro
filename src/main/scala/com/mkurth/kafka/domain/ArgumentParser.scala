package com.mkurth.kafka.domain

trait ArgumentParser {
  def parse(args: List[String]): Option[Config]
}
