package com.venn.stream.api.tableJoin

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

      left("table_join")
      Thread.sleep(1000)
    }
  }

  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

  var idLeft = 0

  def left(topic: String) = {
    val producer = new KafkaProducer[String, String](Common.getProp)
    idLeft = idLeft + 1
    val map = Map("id" -> idLeft, "phone" ->  System.currentTimeMillis(), "date" -> sdf.format(System.currentTimeMillis()))
    val jsonObject: JSONObject = new JSONObject(map)
    println("left : " + jsonObject.toString())
    val msg = new ProducerRecord[String, String](topic, jsonObject.toString())
    producer.send(msg)
    producer.flush()
  }



}

