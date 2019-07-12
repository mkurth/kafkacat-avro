package com.mkurth.kafka.adapter

import com.mkurth.kafka.domain.Decoder
import org.apache.avro.Schema
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

class AvroDecoder(schema: Schema, config: Config) extends Decoder[Array[Byte]] {

  def deserialize(data: Array[Byte], schema: Schema): String = {
    val reader      = new GenericDatumReader[GenericRecord](schema)
    val inputStream = new SeekableByteArrayInput(data)
    val decoder     = DecoderFactory.get.binaryDecoder(inputStream, null)
    reader.read(null, decoder).toString
  }

  def decode(a: Array[Byte]): String = deserialize(a, schema)
}
