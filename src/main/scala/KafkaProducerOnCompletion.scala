import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.log4j.Logger

case class KafkaProducerOnCompletion() extends Callback {

  val logger: Logger = Logger.getLogger(this.getClass.getName.stripSuffix("$"))

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = Option(exception) match {

    case None =>
    case Some(ex) =>
      logger.error("Error occurred during send.", ex)
      throw ex
  }
}
