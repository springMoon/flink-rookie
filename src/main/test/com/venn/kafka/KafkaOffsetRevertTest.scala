package com.venn.kafka

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.venn.common.Common
import com.venn.util.MathUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.parsing.json.JSONObject

/**
  * test data maker
  */

object CurrentDayMaker {


  /**
    * kafka offset revert test
    * kafka offset 回退测试
    *
    * @return
    */
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val producer = new KafkaProducer[String, String](Common.getProp(true))
    var i = 0;
    while (true) {

      //      val map = Map("id"-> i, "createTime"-> sdf.format(System.currentTimeMillis()))
      val map = Map("id" -> i, "createTime" -> sdf.format(System.currentTimeMillis()), "amt" -> (MathUtil.random.nextInt(10) + "." + MathUtil.random.nextInt(10)))
      val jsonObject: JSONObject = new JSONObject(map)
      println(jsonObject.toString())
      // topic current_day
      val msg = new ProducerRecord[String, String]("kafka_offset", jsonObject.toString())
      producer.send(msg)
      producer.flush()
      Thread.sleep(1000)
      i = i + 1
      //      System.exit(-1)
    }
  }

}

