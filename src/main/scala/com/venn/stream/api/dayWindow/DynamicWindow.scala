package com.venn.stream.api.dayWindow

import java.time.Duration
import java.util

import com.google.gson.Gson
import com.venn.entity.{MyStringKafkaRecord, UserLog}
import com.venn.util.{DateTimeUtil, MyKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper
import org.apache.flink.api.scala._
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{ListState, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration

/**
 * 动态窗口实现
 * 数量窗口的逻辑是每个key都有一个属于自己key的数量窗口，
 * 例如：设置一个数量为3的滚动窗口，输入1,2,3,4，不会触发窗口执行，但是继续输入两条1的数据，会输出三个1的数据。
 */
object DynamicWindow {

  def main(args: Array[String]): Unit = {

    // env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = KafkaSource
      .builder[MyStringKafkaRecord]()
      .setBootstrapServers("localhost:9092")
      .setGroupId("MyGroup")
      .setTopics(util.Arrays.asList("user_log"))
      //      .setDeserializer(KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true)))
      .setDeserializer(new MyKafkaRecordDeserializationSchema())
      .setStartingOffsets(OffsetsInitializer.earliest())
      .build()

    env.fromSource(source,
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "kafkaSource")
      .map(new MapFunction[MyStringKafkaRecord, UserLog] {
        override def map(element: MyStringKafkaRecord): UserLog = {

          println("map : " + element.getValue)

          val userLog = new Gson().fromJson(element.getValue, classOf[UserLog]);
          val ts = DateTimeUtil.parse(userLog.getTs)
          userLog.setTimestamp(ts.getTime)
          userLog
        }
      })
      .name("map")
      .uid("map")
      .keyBy(_.getUserId)
      .process(new KeyedProcessFunction[String, UserLog, String] {
        // state for key trigger time
        var timeState: ValueState[Long] = _
        var elementState: ListState[UserLog] = _
        var i = 0


        override def open(parameters: Configuration): Unit = {

          val stateDescriptor = new ValueStateDescriptor[String]("text state", classOf[String])
          timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeState", classOf[Long]))
        }

        override def processElement(value: UserLog, ctx: KeyedProcessFunction[String, UserLog, String]#Context, out: Collector[String]): Unit = {

          elementState.get().forEach(userlog => ++i)

          println("count : " + i)

        }
      })


    env.execute("DynamicWindow")

  }

}
