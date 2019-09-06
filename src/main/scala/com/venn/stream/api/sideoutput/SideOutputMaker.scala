package com.venn.stream.api.sideoutput

import java.text.SimpleDateFormat
import java.util.Calendar

import com.venn.common.Common
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.parsing.json.JSONObject

/**
  * test data maker
  */

object FileSinkMaker {
  val topic = "async"

  def main(args: Array[String]): Unit = {

    while (true) {

      left("side_output")
      Thread.sleep(100)
    }
  }

  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

  var idLeft = 0

  def left(topic: String) = {
    val producer = new KafkaProducer[String, String](Common.getProp)
    idLeft = idLeft + 1
    var str = ""
    if(idLeft%3 == 0){
      str = "apply," + idLeft
    }else if (idLeft%3 == 1){
      str = "result," + idLeft
    }else{
      str = "xx," + idLeft
    }
    println("id : " + idLeft + ", text : " + str)
    val msg = new ProducerRecord[String, String](topic, str)
    producer.send(msg)
    producer.flush()
  }


}

