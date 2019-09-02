package com.venn.stream.api.dayWindow

import java.io.File
import java.text.SimpleDateFormat

import com.venn.common.Common
import com.venn.source.TumblingEventTimeWindows
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
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousEventTimeTrigger, ContinuousProcessingTimeTrigger, CountTrigger}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
  * Created by venn on 19-5-23.
  *
  * use TumblingEventTimeWindows count current day pv
  * for test, update day window to minute window
  *
  *  .windowAll(TumblingEventTimeWindows.of(Time.minutes(1), Time.seconds(0)))
  *  TumblingEventTimeWindows can ensure count o minute event,
  *  and time start at 0 second (like : 00:00:00 to 00:00:59)
  *
  */
object CurrentDayPvCountWaterMark {

  def main(args: Array[String]): Unit = {
    println(1558886400000L - (1558886400000L - 8 + 86400000)%  86400000 )
    // environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    if ("\\".equals(File.pathSeparator)) {
      val rock = new RocksDBStateBackend(Common.CHECK_POINT_DATA_DIR)
      env.setStateBackend(rock)
      // checkpoint interval
      env.enableCheckpointing(10000)
    }

    val topic = "current_day"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val kafkaSource = new FlinkKafkaConsumer[ObjectNode](topic, new JsonNodeDeserializationSchema(), Common.getProp)
    val sink = new FlinkKafkaProducer[String](topic + "_out", new SimpleStringSchema(), Common.getProp)
    sink.setWriteTimestampToKafka(true)

    val stream = env.addSource(kafkaSource)
      .map(node => {
        Eventx(node.get("id").asText(), node.get("createTime").asText())
      })
      /*// use the water marks
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Event]() {
        override def extractTimestamp(event: Event): Long = {
          sdf.parse(event.createTime).getTime
        }
      })*/
      .assignAscendingTimestamps(event => sdf.parse(event.createTime).getTime)
      .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
      .reduce(new ReduceFunction[Eventx] {
        override def reduce(event1: Eventx, event2: Eventx): Eventx = {

//          println("reduce event : " +  event2.toString)
          //            val minId:String = if (event1.id.compareTo(event2.id) >= 0 ) event2.id else event1.id
          //            val maxId = if (event1.id.compareTo(event2.id) < 0 ) event1.id else event2.id
          //            val minCreateTime = if ( event1.createTime.compareTo(event2.createTime) >= 0 ) event2.createTime else event1.createTime
          //            val maxCreateTime = if ( event1.createTime.compareTo(event2.createTime) < 0 ) event1.createTime else event2.createTime
          //            val count = event1.count + event2.count
          //            new EventResult(minId, maxId, minCreateTime, maxCreateTime, count)
          new Eventx(event1.id , event2.id , event1.amt + event2.amt)
        }
      })
      // format output even, connect min max id, add current timestamp
//      .map(event => Event(event.id + "-" + event.createTime, sdf.format(System.currentTimeMillis()), event.count))
    stream.print("result : ")
    // execute job
    env.execute("CurrentDayCount")
  }

}


