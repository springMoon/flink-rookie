//package com.venn.connector.filesink
//
//import java.text.SimpleDateFormat
//import java.util.Calendar
//import com.venn.common.Common
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//
//
///**
//  * test data maker
//  */
//
//object FileSinkMaker {
//  val topic = "async"
//
//  def main(args: Array[String]): Unit = {
//
//    while (true) {
//
//      left("roll_file_sink")
//      Thread.sleep(100)
//    }
//  }
//
//  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
//
//  var idLeft = 0
//
//  def left(topic: String) = {
//    val producer = new KafkaProducer[String, String](Common.getProp)
//    idLeft = idLeft + 1
//    val map = Map("id" -> idLeft, "name" -> ("venn" + System.currentTimeMillis()), "date" -> getCreateTime)
//    val jsonObject: JSONObject = new JSONObject(map)
//    println("left : " + jsonObject.toString())
//    val msg = new ProducerRecord[String, String](topic, jsonObject.toString())
////    producer.send(msg)
////    producer.flush()
//  }
//
//  var minute : Int = 1
//  val calendar: Calendar = Calendar.getInstance()
//  def getCreateTime(): String = {
//    //    minute = minute + 1
//    calendar.add(Calendar.MINUTE, 10)
//    sdf.format(calendar.getTime)
//  }
//
//}
//
