package com.venn.stream.api.sideoutput.lateDataProcess

import java.io.File
import java.text.SimpleDateFormat

import com.venn.index.conf.Common
import com.venn.source.TumblingEventTimeWindows
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * 侧边输出：This operation can be useful when you want to split a stream of data
  */
object LateDataProcess {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if ("/".equals(File.separator)) {
      val backend = new FsStateBackend(Common.CHECK_POINT_DATA_DIR, true)
      env.setStateBackend(backend)
      env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE)
    } else {
      env.setMaxParallelism(1)
      env.setParallelism(1)
    }

    val source = new FlinkKafkaConsumer[ObjectNode]("late_data", new JsonNodeDeserializationSchema(), Common.getProp)
    // 侧边输出的tag
    val late = new OutputTag[LateDataEvent]("late")

    val input = env.addSource(source)
      .map(json => {
        // json : {"id" : 0, "createTime" : "2019-08-24 11:13:14.942", "amt" : "9.8"}
        val id = json.get("id").asText()
        val createTime = json.get("createTime").asText()
        val amt = json.get("amt").asText()
        LateDataEvent("key", id, createTime, amt)
      })
      // assign watermarks every event
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[LateDataEvent]() {
      // check extractTimestamp emitted watermark is non-null and large than previously
      override def checkAndGetNextWatermark(lastElement: LateDataEvent, extractedTimestamp: Long): Watermark = {
        new Watermark(extractedTimestamp)
      }
      // generate next watermark
      override def extractTimestamp(element: LateDataEvent, previousElementTimestamp: Long): Long = {
        val eventTime = sdf.parse(element.createTime).getTime
        eventTime
      }
    })
      // flink auto create watermark
      //      .assignAscendingTimestamps(element => sdf.parse(element.createTime).getTime)
      // assign watermarks periodically(定期生成水印)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LateDataEvent](Time.milliseconds(50)) {
      override def extractTimestamp(element: LateDataEvent): Long = {
        println("current timestamp : " + sdf.parse(element.createTime).getTime)
        sdf.parse(element.createTime).getTime
      }

    })
      // after keyBy will have window number of different key
      .keyBy("key")
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      // get lateData
      .sideOutputLateData(late)
      .process(new ProcessWindowFunction[LateDataEvent, LateDataEvent, Tuple, TimeWindow] {
        // just for debug window process late data
        override def process(key: Tuple, context: Context, elements: Iterable[LateDataEvent], out: Collector[LateDataEvent]): Unit = {
          // print window start timestamp & end timestamp & current watermark time
          println("window:" + context.window.getStart + "-" + context.window.getEnd + ", currentWatermark : " + context.currentWatermark)
          val it = elements.toIterator
          while (it.hasNext) {
            val current = it.next()
            out.collect(current)
          }
        }
      })
      .name("process")
    // print late data
    input.getSideOutput(late).print("late:")
    input.print("apply:")
    env.execute("LateDataProcess")
  }

}

case class LateDataEvent(key: String, id: String, createTime: String, amt: String)
