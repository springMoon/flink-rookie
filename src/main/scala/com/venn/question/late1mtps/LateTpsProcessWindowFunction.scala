package com.venn.question.late1mtps

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

class LateTpsProcessAllWindowFunction[KafkaSimpleStringRecord, String, TimeWindow](lastMinute : Int) extends ProcessAllWindowFunction{

  // for current window
  var windowState: MapState[Int, Long] = _
  // for last window, last senond
  var lastWindow: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {

    windowState = getRuntimeContext.getMapState(new MapStateDescriptor[Int, Long]("window", classOf[Int], classOf[Long]))
    lastWindow = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last", classOf[Long]))
  }


  override def process(context: Context, elements: Iterable[KafkaSimpleStringRecord], out: Collector[String]): Unit = {

  }

  override def clear(context: Context): Unit = {

  }

  override def close(): Unit = {

  }

}
