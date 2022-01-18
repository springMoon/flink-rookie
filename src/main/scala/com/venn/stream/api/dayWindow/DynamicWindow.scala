package com.venn.stream.api.dayWindow

import java.time.Duration
import java.util

import com.google.gson.Gson
import com.venn.common.Common
import com.venn.entity.{MyStringKafkaRecord, UserLog}
import com.venn.util.{DateTimeUtil, MyKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, KafkaSinkBuilder}
import org.apache.flink.connector.kafka.source.{KafkaSource, KafkaSourceBuilder}
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 动态窗口实现
 * 数量窗口的逻辑是每个key都有一个属于自己key的数量窗口，
 * 例如：设置一个数量为3的滚动窗口，输入1,2,3,4，不会触发窗口执行，但是继续输入两条1的数据，会输出三个1的数据。
 */
object DynamicWindow {

  val epochCount = 10
  val epochTime = 1 * 60 * 1000
  val bootstrapServer = "localhost:9092"
  val topic = "user_log"
  val sinkTopic = "user_log_sink"

  def main(args: Array[String]): Unit = {

    // env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = KafkaSource
      .builder[MyStringKafkaRecord]()
      //      new KafkaSourceBuilder[MyStringKafkaRecord]
      .setBootstrapServers(bootstrapServer)
      .setGroupId("MyGroup")
      .setClientIdPrefix("aa")
      .setTopics(util.Arrays.asList("user_log"))
      //      .setDeserializer(KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true)))
      .setDeserializer(new MyKafkaRecordDeserializationSchema())
      //      .setStartingOffsets(OffsetsInitializer.earliest())
      .setStartingOffsets(OffsetsInitializer.latest())
      .build()

    val stream = env.fromSource(source,
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "kafkaSource")
      .map(new MapFunction[MyStringKafkaRecord, UserLog] {
        override def map(element: MyStringKafkaRecord): UserLog = {

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
        var countState: ValueState[Long] = _
        var elementState: ListState[UserLog] = _
        val builder = new StringBuilder


        override def open(parameters: Configuration): Unit = {

          timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeState", classOf[Long]))
          countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
          elementState = getRuntimeContext.getListState(new ListStateDescriptor[UserLog]("elementState", classOf[UserLog]))
        }

        override def processElement(element: UserLog, ctx: KeyedProcessFunction[String, UserLog, String]#Context, out: Collector[String]): Unit = {

          //          println(DateTimeUtil.formatMillis(System.currentTimeMillis(), DateTimeUtil.YYYY_MM_DD_HH_MM_SS) + " - value : " + ctx.getCurrentKey + " - " + countState.value())

          // when first key element enter, init
          if (countState.value() == null) {
            val current = System.currentTimeMillis() + epochTime
            timeState.update(current)
            ctx.timerService().registerEventTimeTimer(current)
            countState.update(1)
            elementState.add(element)
          } else {
            countState.update(countState.value() + 1)
            elementState.add(element)


            // meet the quantity condition
            if (countState.value() >= epochCount) {
              // delete timer
              ctx.timerService().deleteEventTimeTimer(timeState.value())
              // clear count
              countState.clear()
              // process element
              builder.clear()
              elementState.get().forEach(
                userLog => builder.append(userLog.getCategoryId).append("-").append(userLog.getItemId).append(",")
              )
              elementState.clear()
              out.collect(ctx.getCurrentKey + " - " + builder.toString())
            }

            builder.toString
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, UserLog, String]#OnTimerContext, out: Collector[String]): Unit = {

          builder.clear()
          // clear count
          countState.clear()
          // process element
          elementState.get().forEach(
            userLog => builder.append(userLog.getCategoryId).append("-").append(userLog.getItemId).append(",")
          )
          elementState.clear()

          out.collect("timer : " + ctx.getCurrentKey + " - " + builder.toString())

        }
      })
      .name("process")
      .uid("process")

    stream
      .print()
      .name("print")


    val sink = KafkaSink
      .builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setKafkaProducerConfig(Common.getProp)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(sinkTopic)
        .setKeySerializationSchema(new SimpleStringSchema())
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()


    stream.sinkTo(sink)
      .name("sink")
      .uid("sink")

    env.execute("DynamicWindow")

  }

}
