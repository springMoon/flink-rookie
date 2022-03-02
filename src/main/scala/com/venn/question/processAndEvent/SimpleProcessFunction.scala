package com.venn.question.processAndEvent

import com.venn.question.retention.UserLog
import com.venn.util.DateTimeUtil
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * user day retention analyze process function
 */
class SimpleProcessFunction(time: String) extends ProcessWindowFunction[UserLog, String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[UserLog], out: Collector[String]): Unit = {

    val current = DateTimeUtil.formatMillis(System.currentTimeMillis(), DateTimeUtil.YYYY_MM_DD_HH_MM_SS)
    out.collect(current + "\t  time trigger calc: " + time)

  }
}