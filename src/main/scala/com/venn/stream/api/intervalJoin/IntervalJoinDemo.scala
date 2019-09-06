package com.venn.stream.api.intervalJoin

import java.io.File
import java.text.SimpleDateFormat

import com.venn.common.Common
import com.venn.source.TumblingEventTimeWindows
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * interval join demo
  */
object IntervalJoinDemo {

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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val sourceLeft = new FlinkKafkaConsumer[ObjectNode]("topic_left", new JsonNodeDeserializationSchema, Common.getProp)
    val sourceRight = new FlinkKafkaConsumer[ObjectNode]("topic_right", new JsonNodeDeserializationSchema, Common.getProp)

    sourceLeft.setStartFromLatest()
    sourceRight.setStartFromLatest()

    // transfer left stream json to AsyncUser
    val leftStream = env.addSource(sourceLeft)
      .map(json => {
        val id = json.get("id").asText()
        val name = json.get("name").asText()
        val date = json.get("date").asText()
        IntervalUser(id, name, null, date)
      })
      .assignAscendingTimestamps(u => sdf.parse(u.date).getTime)
      .keyBy(0)
    // transfer right stream json to AsyncUser
    val rightStream = env.addSource(sourceRight)
      .map(json => {
        val id = json.get("id").asText()
        val phone = json.get("phone").asText()
        val date = json.get("date").asText()
        IntervalUser(id, null, phone, date)
      })
      .assignAscendingTimestamps(u => sdf.parse(u.date).getTime)
      .keyBy(0)

    // join it
    /*
      左边为主，两边都可以触发,触发范围：
        a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound

     */
    leftStream
      .intervalJoin(rightStream)
      .between(Time.seconds(-2), Time.seconds(7))
      //.lowerBoundExclusive() // 排除下界
//      .upperBoundExclusive() // 排除上界
      .process(new IntervalJoinProcessFunctionDemo)
        /*.assignAscendingTimestamps(_.phone.toLong)
        .keyBy("id")
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .min("id")*/
        /*.reduce(new ReduceFunction[IntervalUser] {
          override def reduce(value1: IntervalUser, value2: IntervalUser): IntervalUser = {
            println("xx -> " + value2)
            value2
          }
        })*/

      .print("result -> ")

    env.execute("IntervalJoinDemo")
  }

}
