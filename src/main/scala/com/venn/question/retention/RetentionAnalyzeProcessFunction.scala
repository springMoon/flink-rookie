package com.venn.question.retention

import java.util.{Calendar, Date}

import com.venn.util.DateTimeUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * user day retention analyze process function
 */
class RetentionAnalyzeProcessFunction extends ProcessWindowFunction[UserLog, String, String, TimeWindow] {

  val LOG = LoggerFactory.getLogger("RetentionAnalyzeProcessFunction")


  /**
   * open: load all user and last day new user
   *
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    LOG.info("RetentionAnalyzeProcessFunction open")
    val calendar = Calendar.getInstance()
    calendar.setTime(new Date())
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    val lastDay = DateTimeUtil.format(calendar.getTime, DateTimeUtil.YYYY_MM_DD)


  }

  /**
   * 1. find current day new user
   * 2. load last day new user
   *
   * @param key
   * @param context
   * @param elements
   * @param out
   */
  override def process(key: String, context: Context, elements: Iterable[UserLog], out: Collector[String]): Unit = {

  }

  /**
   * output current day new user
   *
   * @param context
   */
  override def clear(context: Context): Unit = {
    val window = context.window
    LOG.info(String.format("window start : %s, end: %s, clear", DateTimeUtil.formatMillis(window.getStart, DateTimeUtil.YYYY_MM_DD_HH_MM_SS), DateTimeUtil.formatMillis(window.getEnd, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)))

  }
}
