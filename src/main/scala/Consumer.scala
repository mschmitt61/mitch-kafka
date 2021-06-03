import com.typesafe.config.{Config, ConfigFactory}
import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import collection.JavaConverters._

object Consumer {

  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load("consumer.conf")

    val consumerConf = config.getObject("consumer-config")
    val props = new Properties()

    consumerConf.toConfig.entrySet().forEach(conf => {
      props.put(conf.getKey, conf.getValue.unwrapped)
    })

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

      //val count = records.count()
      //logger.info(s"new batch count: $count")

      records.iterator.forEachRemaining { r =>
        println(s" Partition: ${r.partition()} | Off: ${r.offset} | Key: ${r.key} | Value: ${r.value}")
      }
    }

  }
}
