name := "mitch-kafka"

version := "0.1"

scalaVersion := "2.12.12"

sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue

// Needed to resolve the kafka-avro-serializer dependency
resolvers += "Confluent Repo" at "https://packages.confluent.io/maven"

libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.apache.kafka" % "kafka-clients" % "2.3.0",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.apache.oltu.oauth2" % "org.apache.oltu.oauth2.client" % "1.0.1",
  "com.typesafe" % "config" % "1.2.1",
  "com.github.pureconfig" %% "pureconfig" % "0.13.0",
  "org.apache.avro" % "avro" % "1.7.7",
  "org.apache.kafka" % "kafka_2.11" % "0.10.0.0",
  "io.confluent" % "kafka-avro-serializer" % "6.0.0",
  "org.rogach" %% "scallop" % "4.0.3"
)

mainClass / run := Some("Producer")

