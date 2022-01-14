package com.venn.question.dynamicWindow

import java.util

import com.google.gson.Gson
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
  * data source
  */
class DataSourceFunction extends SourceFunction[String] {

  var flag = true

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    var map = new util.HashMap[String, String]
    while (flag) {

      val random = new Random()
      val gson = new Gson()
      for (i <- 1 to 4) {

        map.put("attr", "attr" + i)
        map.put("value", "" + random.nextInt(1000))
        map.put("time", "" + System.currentTimeMillis())

        val json = gson.toJson(map)

        ctx.collect(json)
      }

      Thread.sleep(1000)

    }

  }

  override def cancel(): Unit = {
    flag = false

  }
}
