package com.venn.question.dynamicWindow

import java.text.SimpleDateFormat
import java.util

import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class DyProcessWindowFunction() extends ProcessWindowFunction[(DataEntity, Command), String, String, TimeWindow] {

  val logger = LoggerFactory.getLogger("DyProcessWindowFunction")
  var gson: Gson = _


  override def open(parameters: Configuration): Unit = {
    gson = new Gson()
  }

  override def process(key: String, context: Context, elements: Iterable[(DataEntity, Command)], out: Collector[String]): Unit = {
    // start-end
    val taskId = elements.head._2.taskId
    val method = elements.head._2.method
    val targetAttr = elements.head._2.targetAttr
    val periodStartTime = context.window.getStart
    val periodEndTime = context.window.getEnd

    var value: Double = 0d
    method match {
      case "sum" =>
        value = 0d
      case "min" =>
        value = Double.MaxValue
      case "max" =>
        value = Double.MinValue
      case _ =>
        logger.warn("input method exception")
        return
    }

    val it = elements.toIterator
    while (it.hasNext) {
      val currentValue = it.next()._1.value
      method match {
        case "sum" =>
          value += currentValue
        case "count" =>
          value += 1
        case "min" =>
          if (currentValue < value) {
            value = currentValue
          }
        case "max" =>
          if (currentValue > value) {
            value = currentValue
          }
        case _ =>
      }
    }

    val sdf = new SimpleDateFormat("HH:mm:ss")
    val resultMap = new util.HashMap[String, String]
    resultMap.put("taskId", taskId)
    resultMap.put("method", method)
    resultMap.put("targetAttr", targetAttr)
    resultMap.put("periodStartTime", sdf.format(periodStartTime))
    resultMap.put("periodEndTime", sdf.format(periodEndTime))
    resultMap.put("value", value.toString)

    out.collect(gson.toJson(resultMap))
  }
}
