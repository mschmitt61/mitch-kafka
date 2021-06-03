import Producer.getClass
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties
import scala.io.Source

object Producer {

  type messagesList = List[ProducerRecord[String, User]]

  def main(args: Array[String]): Unit = {

    val config: Config = ConfigFactory.load("producer.conf")

    val producerConf = config.getObject("producer-config")
    val props = new Properties()

    producerConf.toConfig.entrySet().forEach(conf => {
      props.put(conf.getKey, conf.getValue.unwrapped)
    })

    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])

    runProducer(props)
  }

  def runProducer(props: Properties): Unit = {
    val producer = new KafkaProducer[String, User](props)

    val schema: Schema = new Parser().parse(Source.fromURL(getClass.getResource("/src/main/avro/userSchema.avsc")).mkString)

    val producerFunc: (messagesList) => Unit = producerFunction(_, producer)

    // This will send x "batches" of data using the users api. It will then send each individual message
    // to Kafka. Number of messages sent is totalBatch * the average of random users generated each api call
    // (between 1 and 10)
    0 until props.getProperty("totalAPICalls").toInt foreach { _ =>
      // Add a sleep to continually send messages, but kinda slow.
      val sleepTime = props.getProperty("sleepBetweenSendMs").toInt
      Thread.sleep(sleepTime)
      UserGenerator.produceRandomUserData(producerFunc,schema)
    }
  }

  def producerFunction(messages: messagesList, kafkaProducer: KafkaProducer[String, User]): Unit = {

    //val record = new ProducerRecord[String, String](topic, key, message)
    //println(s"Partition: ${record.partition()} | Key: ${record.key} | Value: ${record.value}")
    messages.foreach(message => {
      println(s"Partition: ${message.partition()} | Key: ${message.key} | Value: ${message.value}")
      kafkaProducer.send(message)
    })
    kafkaProducer.flush()
    kafkaProducer.close()
    //kafkaProducer.send(messages: _*)//, KafkaProducerOnCompletion()).get()
  }

  //case class User(first: String, last: String, email: Option[String], dateOfBirth: Option[String])
  final case class User(first: String, last: String, email: Option[String], dateOfBirth: Option[String]) extends SpecificRecord {
    override def put(i: Int, v: Any): Unit = Unit

    override def get(i: Int): AnyRef = ""

    override def getSchema: Schema = new Parser().parse(Source.fromURL(getClass.getResource("/userSchema.avsc")).mkString)
  }

  object User {
    val SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
      Source.fromURL(getClass.getResource("/userSchema.avsc")).mkString)
  }

}
