package com.venn.question.late1mtps

import com.venn.util.DateTimeUtil
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._

import java.util

/**
 * 不固定长度的输出间隔
 * @param windowSize
 * @param intervalSize
 */
class AdjustLateTpsProcessAllWindowFunction(windowSize: Int, intervalSize: Int) extends ProcessAllWindowFunction[(String, Long), (String, String, Int, String, Double), TimeWindow] {

  val LOG = LoggerFactory.getLogger("LateTpsSecondProcessAllWindowFunction")
  // for current window
  //  var windowState: MapState[Int, Long] = _
  // for last window, last senond
  var lastWindow: ValueState[util.HashMap[Int, Long]] = _
  var interval: Int = _
  var tag: OutputTag[String] = _


  override def open(parameters: Configuration): Unit = {

    //    windowState = getRuntimeContext.getMapState(new MapStateDescriptor[Int, Long]("window", classOf[Int], classOf[Long]))
    lastWindow = getRuntimeContext.getState(new ValueStateDescriptor[util.HashMap[Int, Long]]("last", classOf[util.HashMap[Int, Long]]))

    interval = windowSize

    tag = new OutputTag[String]("size")
  }

  override def process(context: Context, elements: Iterable[(String, Long)], out: Collector[(String, String, Int, String, Double)]): Unit = {

    // get window
    val windowStart = DateTimeUtil.formatMillis(context.window.getStart, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)
    val windowEnd = DateTimeUtil.formatMillis(context.window.getEnd, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)
    // get last window state map, for last window over size date
    var lastWindowStateMap = lastWindow.value()
    // init lastWindow state as zero
    if (lastWindowStateMap == null) {
      lastWindowStateMap = initLastWindowState
    }

    // init tps currentWindowMap  0 - 3600
    val currentWindowMap = new util.HashMap[Int, Long]()
    // init tps next window map 3600 - 3660
    val nextWindowMap = new util.HashMap[Int, Long]()
    for (i <- 0 until interval) {
      currentWindowMap.put(i, 0)
    }
    for (i <- interval - 60 until interval) {
      nextWindowMap.put(i, 0)
    }

    elements.foreach((e: (String, Long)) => {
      // 获取每天数据在1小时内的秒数
      val current: Int = (e._2 / 1000 % interval).toInt
      currentWindowMap.put(current, currentWindowMap.get(current) + 1)
    })
    // load next window data
    for(i <- interval - 60 until interval){
      nextWindowMap.put(i, currentWindowMap.get(i))
    }

    // todo tmp to side
    currentWindowMap.forEach((a: Int, b: Long) => {
      //      context.output(tag, windowStart + "," + windowEnd + "," + a + "," + b)
      context.output(tag, windowStart + ", 1," + a + "," + b)
    })
    nextWindowMap.forEach((a: Int, b: Long) => {
      context.output(tag, windowStart + ", 2," + a + "," + b)
    })

    // calculate every window size
    for (window <- 0 until interval / intervalSize) {
      // load current interval tps
      // 计算 每个窗口的时间范围
      val (start, end) = calWindowStartEnd(window)
      var size = 0l
      for (j <- start until end) {
        // if window second include -60 to 0, add last window state
        if (j <= 0) {
          size += lastWindowStateMap.get(interval + j)
        }
        if (currentWindowMap.containsKey(j)) {
          size += currentWindowMap.get(j)
        }
      }
      out.collect(windowStart, windowEnd, window, start + "-" + end, size / 60.0)

    }
    // clear last window
    lastWindow.clear()
    // keep last
    lastWindow.update(nextWindowMap)
  }

  // init last window state as zero
  private def initLastWindowState: util.HashMap[Int, Long] = {
    val map = new util.HashMap[Int, Long]()
    for (i <- 0 until 60) {
      map.put(i, 0)
    }
    map
  }

  // calculate window start and end
  def calWindowStartEnd(i: Int): (Int, Int) = {
    val end = i * intervalSize
    val start = end - 60
    (start, end)
  }

  override def close(): Unit = {
    lastWindow.clear()
  }

}
