package com.venn.stream.api.dayWindow

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.venn.common.Common
import com.venn.flink.util.MathUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.parsing.json.JSONObject

/**
  * test data maker
  */

object CurrentDayMaker {


  var minute : Int = 1
  val calendar: Calendar = Calendar.getInstance()

  /**
    * 一天时间比较长，不方便观察，将时间改为当前时间，
    * 每次累加10分钟，这样一天只需要144次循环，也就是144秒
    * @return
    */
  def getCreateTime(): String = {
//    minute = minute + 1
    calendar.add(Calendar.MINUTE, 10)
    sdf.format(calendar.getTime)
  }
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val producer = new KafkaProducer[String, String](Common.getProp)
    calendar.setTime(new Date())
    println(sdf.format(calendar.getTime))
    var i =0;
    while (true) {

//      val map = Map("id"-> i, "createTime"-> sdf.format(System.currentTimeMillis()))
      val map = Map("id"-> i, "createTime"-> getCreateTime(), "amt"-> (MathUtil.random.nextInt(10) +"." + MathUtil.random.nextInt(10)))
      val jsonObject: JSONObject = new JSONObject(map)
      println(jsonObject.toString())
      // topic current_day
      val msg = new ProducerRecord[String, String]("current_day", jsonObject.toString())
      producer.send(msg)
      producer.flush()
      Thread.sleep(1000)
      i = i + 1
//      System.exit(-1)
    }
  }

}

