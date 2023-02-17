package com.venn.question.late1mtps

import com.venn.util.DateTimeUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.util

/**
 * 整分钟输出间隔的窗口
 * @param windowSize
 * @param intervalSize
 */
class FixedLateTpsProcessAllWindowFunction(windowSize: Int, intervalSize: Int) extends ProcessAllWindowFunction[(String, Long), (String, String, Int, Double), TimeWindow] {

  // for last window, last senond
  var lastWindow: ValueState[Double] = _
  var interval: Int = _

  override def open(parameters: Configuration): Unit = {

    //    windowState = getRuntimeContext.getMapState(new MapStateDescriptor[Int, Long]("window", classOf[Int], classOf[Long]))
    lastWindow = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last", classOf[Double]))

    interval = windowSize / intervalSize
  }

  override def process(context: Context, elements: Iterable[(String, Long)], out: Collector[(String, String, Int, Double)]): Unit = {

    // get window
    val windowStart = DateTimeUtil.formatMillis(context.window.getStart, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)
    val windowEnd = DateTimeUtil.formatMillis(context.window.getEnd, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)
    var lastWindowCount = lastWindow.value()
    if (lastWindowCount == null) {
      lastWindowCount = 0
    }

    // init tps map
    val map = new util.HashMap[Int, Long]()
    for (_ <- 0 to windowSize - 1) {
      map.put(0, 0)
    }

    // for each element, get every window size
    elements.foreach((e: (String, Long)) => {
      val current: Int = (e._2 / 1000 % interval).toInt
      map.put(current, map.get(current) + 1)
    })

    // for every zero window, out last window count
    out.collect(windowStart, windowEnd, 0, lastWindowCount)
    for (i <- 0 until interval - 1) {
      out.collect(windowStart, windowEnd, i + 1, map.get(i + 1) / 60.0)
    }

    // keep window last minute count as next window zero window count
    lastWindow.update(map.get(interval - 1) / 60.0)

  }

  override def close(): Unit = {
    lastWindow.clear()
  }

}
