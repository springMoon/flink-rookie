package com.venn.connector.jdbcOutput

import java.text.SimpleDateFormat

import com.venn.common.Common
import com.venn.util.{MathUtil, StringUtil}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * test data maker
  */

object MysqlOutputMaker {
  val topic = "async"

  def main(args: Array[String]): Unit = {

    while (true) {

      left("mysql_output")
      Thread.sleep(100)
    }
  }

  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

  var id = 0

  def left(topic: String) = {
    val producer = new KafkaProducer[String, String](Common.getProp)
    id = id + 1
    val username = StringUtil.getRandomString(5)
    val password = StringUtil.getRandomString(10)
    val sex = MathUtil.random.nextInt(2)
    val phone = MathUtil.getRadomNum(11)

    val message = username + "," + password + "," + sex + "," + phone

    val msg = new ProducerRecord[String, String](topic, message)
    producer.send(msg)
    producer.flush()
    println("send : " + message)
  }


}

