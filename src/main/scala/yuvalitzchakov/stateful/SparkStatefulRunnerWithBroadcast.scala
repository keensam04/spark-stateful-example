package yuvalitzchakov.stateful

import java.util.Properties

import argonaut.Argonaut._
import com.typesafe.config.{Config, ConfigFactory}
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import yuvalitzchakov.user.{UserEvent, UserSession}
import yuvalitzchakov.utils.{CredentialsProvider, KafkaWriter}

import scala.collection.mutable
import scalaz.{-\/, \/-}

/**
  * Created by Yuval.Itzchakov on 3/12/2017.
  */
object SparkStatefulRunnerWithBroadcast {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-stateful-example")

    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val prop = new Properties()
    prop.setProperty("vcapServices", System.getenv("VCAP_SERVICES"))
    prop.setProperty("checkpointDir", "\"file:///tmp/checkpoint/spark/stateful-example/\"")
    prop.setProperty("batchDuration", "4000") // in milliseconds
    prop.setProperty("timeoutInMinutes", "5")
    prop.setProperty("rawMsgKafkaTopic", System.getenv("RAW_MSG_TOPIC"))

    val config: Config = ConfigFactory.parseString(prop.toString)
    val sc = new SparkContext(sparkConf)
    runJob(sc, config)
  }

  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val checkpointDir = jobConfig.getString("checkpointDir")
    val batchDuration = jobConfig.getLong("batchDuration")

    val ssc = StreamingContext.getOrCreate(checkpointDir, () =>  {

      println("creating context newly")

      // clear if any files are left in the checkpoint directory before creating the context newly
      clearCheckpoint(checkpointDir)

      val streamingContext = new StreamingContext(sc, Milliseconds(batchDuration))
      streamingContext.checkpoint(checkpointDir)

      /* define the lineage on the streaming context only when created newl - when recreated from the checkpoint directory,
       * the lineage is recreated from the stored checkpoint information
       */
      defineLineage(streamingContext, jobConfig)

      streamingContext
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def defineLineage(ssc: StreamingContext, jobConfig: Config): Unit = {

    val credentialsStore = new CredentialsProvider(jobConfig.getConfig("vcapServices"))
    val kafkaCredentials = credentialsStore.fetchCredentialByNameAsJson("kafka-service").get
    val brokerUrl = CredentialsProvider.jsonArrayToConnection(kafkaCredentials.get("brokers").getAsJsonArray)

    val kafkaWriter = KafkaWriter.getInstance(brokerUrl)
    val stateSpec = StateSpec.function((key: Int, value: Option[UserEvent], state: State[UserSession]) =>
      updateUserEvents(key, value, state, kafkaWriter)).timeout(Minutes(jobConfig.getLong("timeoutInMinutes")))

    val offsetsQueue = mutable.Queue[Array[OffsetRange]]()

    var kafkaParams: Map[String, String] = Map()
    kafkaParams += ("bootstrap.servers" -> brokerUrl)

    val kafkaTextStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(jobConfig.getString("rawMsgKafkaTopic")))

    kafkaTextStream
      .transform(rdd => {
        offsetsQueue.enqueue(rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
        rdd
      })
      .map(deserializeUserEvent)
      .filter(_ != UserEvent.empty)
      .mapWithState(stateSpec)
      .foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          rdd.foreach(maybeUserSession => maybeUserSession.foreach {
            userSession =>
              // Store user session here
              println(userSession)
          })
        }
      }
  }


  private def clearCheckpoint(checkpointDir: String): Unit = {
    // clear all files from the checkpoint directory
    val checkpointPath = new Path(checkpointDir)
    val fs = checkpointPath.getFileSystem(new Configuration())
    fs.delete(checkpointPath, true)
  }

  def deserializeUserEvent(json: (String,String)): (Int, UserEvent) = {
    json._2.decodeEither[UserEvent] match {
      case \/-(userEvent) =>
        (userEvent.id, userEvent)
      case -\/(error) =>
        println(s"Failed to parse user event: $error")
        (UserEvent.empty.id, UserEvent.empty)
    }
  }

  def updateUserEvents(key: Int,
                       value: Option[UserEvent],
                       state: State[UserSession],
                       kafkaWriter: Broadcast[KafkaWriter]): Option[UserSession] = {
    def updateUserSessions(newEvent: UserEvent): Option[UserSession] = {
      val existingEvents: Seq[UserEvent] =
        state
          .getOption()
          .map(_.userEvents)
          .getOrElse(Seq[UserEvent]())

      val updatedUserSessions = UserSession(newEvent +: existingEvents)

      updatedUserSessions.userEvents.find(_.isLast) match {
        case Some(_) =>
          state.remove()
          kafkaWriter.value.writeToOutTopic(s"state removed. removed value is ${updatedUserSessions.userEvents.last}")
          Some(updatedUserSessions)
        case None =>
          state.update(updatedUserSessions)
          kafkaWriter.value.writeToOutTopic(s"state updated. removed value is ${state.get.userEvents.last}")
          None
      }
    }

    value match {
      case Some(newEvent) => updateUserSessions(newEvent)
      case _ if state.isTimingOut() => state.getOption()
    }
  }
}
