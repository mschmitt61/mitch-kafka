import Producer.{User, messagesList}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, TrustSelfSignedStrategy}
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContexts
import org.apache.http.util.EntityUtils
import org.apache.log4j.Logger
import org.json4s.{DefaultFormats, JArray, JsonAST}
import org.json4s.native.JsonMethods.{compact, parse, render}
import kafka.producer.KeyedMessage
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.ByteArrayOutputStream
import scala.language.postfixOps
import scala.util.Random

object UserGenerator {

  val logger: Logger = Logger.getLogger(this.getClass.getName.stripSuffix("$"))

  implicit val formats: DefaultFormats.type = DefaultFormats

  val client: HttpClient = {
    val trustSelfSignedStrategy = new TrustSelfSignedStrategy()
    val sslContext = SSLContexts
      .custom()
      .loadTrustMaterial(null, trustSelfSignedStrategy)
      .build()
    val sslConnection = new SSLConnectionSocketFactory(sslContext)
    HttpClients.custom().setSSLSocketFactory(sslConnection).build()
  }

  def produceRandomUserData(producerFunction: (messagesList) => Unit, schema: Schema): Unit = {
    // Get a random number of users between 1 and 10 to send to kafka.
    val usersToCreate = (Math.random() * 10).toInt + 1
    val url = s"https://randomuser.me/api/?results=$usersToCreate"

    val companyList = List("Company1", "Company2", "Company3",
      "Company4", "Company5")
    val get = new HttpGet(url)

    val response = client.execute(get)
    Option(response.getStatusLine) match {

      case None => throw new Error(s"Status line is null")

      case Some(line) if line.getStatusCode == 200 => {
        val body = EntityUtils.toString(response.getEntity)

        val results = parse(body) \ "results"

        val messages: List[ProducerRecord[String, User]] = results.extract[JArray].toOption.map({
          case d: JArray => d.arr map { row =>
            val firstName = (row \ "name" \ "first").asInstanceOf[JsonAST.JString].s
            val lastName = (row \ "name" \ "last").asInstanceOf[JsonAST.JString].s
            val email = (row \ "email").asInstanceOf[JsonAST.JString].s
            val dob = (row \ "dob" \ "date").asInstanceOf[JsonAST.JString].s

            // If an even DOB month, set month to null
            val userDOB = if (dob.split('-')(1).toInt % 2 == 0) null
            else dob

            val user = User(firstName,lastName, email, userDOB)
            val companyKey = companyList(Random.nextInt(companyList.length))

            new ProducerRecord[String, User]("USER", companyKey, user)
          }
        }).get

        producerFunction(messages)
      }
    }
  }
}
