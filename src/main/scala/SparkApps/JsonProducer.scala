package SparkApps
import java.util.Properties

//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig,ProducerRecord}

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}

object JsonProducer {
  def main(args:Array[String]):Unit = {
    val props = new Properties()

    props.put("bootstrap.servers","localhost:9092")
    props.put("acks","all")
    props.put("client.id","ProducerApp")
    props.put("retries","4")
    props.put("batch.size","32768")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val topic = "jsonTopic"
    val producer = new KafkaProducer[String,String](props)
    val file = scala.io.Source.fromFile("/Users/kirand/Documents/nameList.json")


    for (line <- file.getLines()) {
      val msg = new ProducerRecord[String,String](topic,line)
      producer.send(msg)
    }
    producer.close()


  }
}