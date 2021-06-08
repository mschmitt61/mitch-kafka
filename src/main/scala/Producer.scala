import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.avro.generic.GenericRecord
import org.rogach.scallop._

import java.util.Properties
import scala.io.Source

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  // user or date
  val schema = opt[String](required = true)
  verify()
}

object Producer {

  type messagesList = List[ProducerRecord[String, GenericRecord]]

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    val dataType = conf.schema.getOrElse("")

    val config: Config = ConfigFactory.load("producer.conf")

    val producerConf = config.getObject("producer-config")
    val props = new Properties()

    producerConf.toConfig.entrySet().forEach(conf => {
      props.put(conf.getKey, conf.getValue.unwrapped)
    })

    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])

    val schema: Schema = new Parser().parse(Source.fromURL(getClass.getResource(s"/${dataType}Schema.avsc")).mkString)

    runProducer(props, schema)
  }

  def runProducer(props: Properties, schema: Schema): Unit = {
    // TODO look into avro4s
    val producer = new KafkaProducer[String, GenericRecord](props)


    val producerFunc: (messagesList) => Unit = producerFunction(_, producer)

    // This will send x "batches" of data using the users api. It will then send each individual message
    // to Kafka. Number of messages sent is totalBatch * the average of random users generated each api call
    // (between 1 and 10)
    0 until props.getProperty("totalAPICalls").toInt foreach { _ =>
      // Add a sleep to continually send messages, but kinda slow.
      val sleepTime = props.getProperty("sleepBetweenSendMs").toInt
      Thread.sleep(sleepTime)
      DataGenerator.produceRandomUserData(producerFunc, schema)
    }
  }

  def producerFunction(messages: messagesList, kafkaProducer: KafkaProducer[String, GenericRecord]): Unit = {

    messages.foreach(message => {
      println(s"Partition: ${message.partition()} | Key: ${message.key} | Value: ${message.value}")
      kafkaProducer.send(message)
    })
  }

}
