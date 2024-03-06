package com.venn.demo

import com.google.gson.GsonBuilder

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.venn.common.Common
import com.venn.util.MathUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/**
  * test data maker
  */

object SlotPartitionMaker {

  var minute: Int = 1
  val calendar: Calendar = Calendar.getInstance()
  /**
    * 一天时间比较长，不方便观察，将时间改为当前时间，
    * 每次累加10分钟，这样一天只需要144次循环，也就是144秒
    *
    * @return
    */
  def getCreateTime(): String = {
    //    minute = minute + 1
    calendar.add(Calendar.MILLISECOND, 10)
    sdf.format(calendar.getTime)
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {

    val prop = Common.getProp
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](Common.getProp)
    calendar.setTime(new Date())
    println(sdf.format(calendar.getTime))
    var i = 0;
    while (true) {
      val map = Map("id" -> i, "createTime" -> getCreateTime(), "amt" -> (MathUtil.random.nextInt(10) + "." + MathUtil.random.nextInt(10)))
      val gson = new GsonBuilder().create();
      gson.toJson(map);
      println(gson.toString())
      // topic current_day
      val msg = new ProducerRecord[String, String]("slot_partition", gson.toString())
      producer.send(msg)
      producer.flush()
      if (MathUtil.random.nextBoolean()) {
        Thread.sleep(1500)
      } else {
        Thread.sleep(500)

      }
      i = i + 1
      //      System.exit(-1)
    }
  }

}

