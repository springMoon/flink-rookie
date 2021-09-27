package com.venn.stream.api.dayWindow

import java.io.File
import java.text.SimpleDateFormat

import com.venn.common.Common
import com.venn.source.TumblingEventTimeWindows
import com.venn.util.CheckpointUtil
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousEventTimeTrigger, ContinuousProcessingTimeTrigger}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
 * Created by venn on 19-5-23.
 *
 * use TumblingEventTimeWindows count current day pv
 * for test, update day window to minute window
 *
 * .windowAll(TumblingEventTimeWindows.of(Time.minutes(1), Time.seconds(0)))
 * TumblingEventTimeWindows can ensure count o minute event,
 * and time start at 0 second (like : 00:00:00 to 00:00:59)
 *
 */
object CurrentDayPvCount {

  def main(args: Array[String]): Unit = {
    // environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //    if ("\\".equals(File.pathSeparator)) {
    //      val rock = new RocksDBStateBackend(Common.CHECK_POINT_DATA_DIR)
    //      env.setStateBackend(rock)
    //      // checkpoint interval
    //      env.enableCheckpointing(10000)
    //    }
    CheckpointUtil.setCheckpoint(env, "rocksdb", Common.CHECK_POINT_DATA_DIR, 10)

    val topic = "current_day"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val kafkaSource = new FlinkKafkaConsumer[ObjectNode](topic, new JsonNodeDeserializationSchema(), Common.getProp)
    val sink = new FlinkKafkaProducer[String](topic + "_out", new SimpleStringSchema(), Common.getProp)
    sink.setWriteTimestampToKafka(true)

    val stream = env.addSource(kafkaSource)
      .map(node => {
        Eventx(node.get("id").asText(), node.get("createTime").asText())
      })
      .assignAscendingTimestamps(event => sdf.parse(event.createTime).getTime)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Eventx](Time.seconds(60)) {
        override def extractTimestamp(element: Eventx): Long = {
          sdf.parse(element.createTime).getTime
        }
      })
      // window is one minute, start at 0 second
      //.windowAll(TumblingEventTimeWindows.of(Time.minutes(1), Time.seconds(0)))
      // window is one hour, start at 0 second
      //      .windowAll(TumblingEventTimeWindows.of(Time.hours(1), Time.seconds(0)))
      // window is one day, start at 0 second, todo there have a bug(FLINK-11326), can't use negative number, 1.8 修复
      //      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
      // every event one minute 如果使用了trigger，窗口函数每次执行，窗口中的所有元素都会参与计算
      //      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(3800)))
      // every process one minute
      .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
      // every event, export current value,
      //      .trigger(CountTrigger.of(1))
      .reduce(new ReduceFunction[Eventx] {


        override def reduce(event1: Eventx, event2: Eventx): Eventx = {
          print(event2.toString)

          // 将结果中，id的最小值和最大值输出
          new Eventx(event1.id, event2.id, event1.amt + event2.amt)
        }
      })
    // format output even, connect min max id, add current timestamp
    //      .map(event => Event(event.id + "-" + event.createTime, sdf.format(System.currentTimeMillis()), event.count))
    stream.print("result : ")

    // execute job
    env.execute("CurrentDayCount")
  }

}

case class Event(id: String, createTime: String, count: Int = 1) {}

