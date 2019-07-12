name := "kafkacat-avro"

version := "0.1"

scalaVersion := "2.13.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.12" % "2.2.1"
libraryDependencies += "org.apache.avro"  % "avro"       % "1.9.0"
libraryDependencies += "com.github.scopt" %% "scopt"     % "4.0.0-RC2"
