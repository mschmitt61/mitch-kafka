import com.typesafe.config.{Config, ConfigFactory}
import io.confluent.kafka.serializers.KafkaAvroDeserializer

import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.consumer.ConsumerConfig

import collection.JavaConverters._

object Consumer {

  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load("consumer.conf")

    val consumerConf = config.getObject("consumer-config")
    val props = new Properties()

    consumerConf.toConfig.entrySet().forEach(conf => {
      props.put(conf.getKey, conf.getValue.unwrapped)
    })

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])

    runConsumer(props)

  }

  def runConsumer(consumerConf: Properties): Unit = {

    // Need to convert to java collection for usability with consumer.subscribe
    val topics = (consumerConf.get("consumer.topics").toString :: Nil).asJavaCollection

    val consumer = new KafkaConsumer[String, String](consumerConf)

    val pollMs = Duration.ofMillis(consumerConf.get("consumer.poll.ms").toString.toInt)

    consumer.subscribe(topics)

    while (true) {
      println("Consumer started, awaiting records...")
      val records: ConsumerRecords[String, String] = consumer.poll(pollMs)

      records.iterator.forEachRemaining { r =>
        println(s" Partition: ${r.partition()} | Off: ${r.offset} | Key: ${r.key} | Value: ${r.value}")
      }
    }

  }
}
