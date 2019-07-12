package com.mkurth.kafka

import org.apache.avro.Schema
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

object AvroDecoder {

  def deserialize(data: Array[Byte], schema: Schema) = {
    val reader      = new GenericDatumReader[GenericRecord](schema)
    val inputStream = new SeekableByteArrayInput(data)
    val decoder     = DecoderFactory.get.binaryDecoder(inputStream, null)
    reader.read(null, decoder).toString
  }

}
