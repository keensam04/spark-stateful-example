package yuvalitzchakov.utils

import java.util

import com.google.gson.{Gson, JsonArray, JsonObject}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
  * Created by I311125 on 7/31/2017.
  */
class CredentialsProvider(vcapServices: Config) extends Serializable {

  // constructor
  private val userProvCredentials = vcapServices.getConfigList("user-provided")
  @transient private lazy val gson = new Gson

  // parses only UPS - temporary
  def fetchCredentialByNameAsJson(name: String): Option[JsonObject] = {
    val credentialByName = fetchCredentialsAsMap(name)
    if (credentialByName.isDefined)
      Some(gson.fromJson(gson.toJson(credentialByName.get), classOf[JsonObject]))
    else None
  }

  def fetchCredentialsByNameAsConfig(name: String): Option[Config] = {
    val credentialByName = fetchCredentialsAsMap(name)
    if (credentialByName.isDefined)
      Some(ConfigFactory.parseMap(credentialByName.get))
    else
      None
  }

  private def fetchCredentialsAsMap(name: String): Option[java.util.Map[String, String]] = {
    val filteredConfig = userProvCredentials.asScala.find(config => config.getString("name").equals(name))

    if(filteredConfig.isDefined)
      Some(filteredConfig.get.getAnyRef("credentials").asInstanceOf[util.HashMap[String, String]])
    else None
  }
}

object CredentialsProvider {
  def jsonArrayToConnection(array: JsonArray): String = {
    array.iterator.toIterator.toArray.map(ele => ele.getAsJsonObject).
      map(ele => ele.get("hostname").getAsString + ":" + ele.get("port").getAsInt).mkString(",")
  }
}
