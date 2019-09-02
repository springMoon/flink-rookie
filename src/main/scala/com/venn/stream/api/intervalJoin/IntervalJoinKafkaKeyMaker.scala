package com.venn.stream.api.intervalJoin

import java.text.SimpleDateFormat

import com.venn.common.Common
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.parsing.json.JSONObject

/**
  * test data maker
  */

object IntervalJoinKafkaKeyMaker {
  val topic = "async"

  def main(args: Array[String]): Unit = {

    while (true) {

      left("topic_left")
      right("topic_right")
      Thread.sleep(500)
    }
  }

  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

  var idLeft = 0

  def left(topic: String) = {
    val producer = new KafkaProducer[String, String](Common.getProp)
    idLeft = idLeft + 1
    val map = Map("id" -> idLeft, "name" -> ("venn" + System.currentTimeMillis()), "date" -> sdf.format(System.currentTimeMillis()))
    val jsonObject: JSONObject = new JSONObject(map)
    println("left : " + jsonObject.toString())
    val msg = new ProducerRecord[String, String](topic, jsonObject.toString())
    producer.send(msg)
    producer.flush()
  }

  var idRight = 0

  def right(topic: String) = {
    val producer = new KafkaProducer[String, String](Common.getProp)
    idRight = idRight + 1
    val map = Map("id" -> idRight,  "phone" -> ("17713333333" + idRight), "date" -> sdf.format(System.currentTimeMillis()))
    val jsonObject: JSONObject = new JSONObject(map)
    println("right : \t\t\t\t\t\t\t\t" + jsonObject.toString())
    val msg = new ProducerRecord[String, String](topic, jsonObject.toString())
    producer.send(msg)
    producer.flush()
  }

}

