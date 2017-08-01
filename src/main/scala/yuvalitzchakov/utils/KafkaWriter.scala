package yuvalitzchakov.utils

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * Created by I077650 on 5/15/2017.
  */
class KafkaWriter(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  private val instanceUUID = UUID.randomUUID().toString

  @transient private lazy val kafkaProducer: KafkaProducer[String, String] = {
    if (KafkaWriter.kafkaProducer == null) {
      KafkaWriter.kafkaProducer = createProducer()
    }
    KafkaWriter.kafkaProducer
  }

  def writeToOutTopic(outMessage: String): Unit = {
    kafkaProducer.send(new ProducerRecord[String, String]("out-spark-stateful-example", outMessage), new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception) = {
        println(s"Message sent to topic ${recordMetadata.topic()} at offset ${recordMetadata.offset()}")
      }
    })
  }
}

object KafkaWriter {
  @volatile var kafkaErrorWriterBroadcast: Broadcast[KafkaWriter] = _
  @transient private var kafkaProducer: KafkaProducer[String, String] = _

  def getInstance(brokerConnectionUrl: String): Broadcast[KafkaWriter] = {

    if (kafkaErrorWriterBroadcast == null) {
      synchronized {
        if (kafkaErrorWriterBroadcast == null) {
          val createProducer = () => {
            val producerProp = new Properties
            producerProp.put("client.id", "sparkStatefuleExampleResult")
            producerProp.put("serializer.class", "kafka.serializer.StringEncoder")
            producerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            producerProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            producerProp.put("producer.type", "async")
            producerProp.put("bootstrap.servers", brokerConnectionUrl)

            val producer = new KafkaProducer[String, String](producerProp)

            sys.addShutdownHook(() => {
              println("Shutting down kafka producer per executor")
              producer.close()
            })
            producer
          }
          val kafkaErrorWriter = new KafkaWriter(createProducer)
          kafkaErrorWriterBroadcast = SparkContext.getOrCreate().broadcast(kafkaErrorWriter)
        }
      }
    }
    kafkaErrorWriterBroadcast
  }
}



