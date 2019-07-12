name         := "kafkacat-avro"
version      := "0.1"
maintainer   := "michael.kurth@springernature.com"
scalaVersion := "2.13.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.12"   % "2.3.0"
libraryDependencies += "org.apache.avro"  % "avro"         % "1.9.0"
libraryDependencies += "com.github.scopt" %% "scopt"       % "4.0.0-RC2"
libraryDependencies += "org.slf4j"        % "slf4j-simple" % "1.7.26"

buildInfoKeys    := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "com.mkurth.kafka"

enablePlugins(JavaAppPackaging, BuildInfoPlugin)
