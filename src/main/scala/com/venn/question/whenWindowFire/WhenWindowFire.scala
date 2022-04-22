package com.venn.question.whenWindowFire

import com.google.gson.{JsonObject, JsonParser}
import com.venn.common.Common
import com.venn.entity.{Behavior, StreamElement}
import com.venn.util.DateTimeUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssignerSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util

/**
 * for blog: Flink 窗口结束前能触发多少次
 */
object WhenWindowFire {

  val LOG = LoggerFactory.getLogger("WhenWindowFire")

  def main(args: Array[String]): Unit = {

    val topic = "user_log"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setTopics(topic)
//      .setGroupId()
      //      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build();

    // WatermarkStrategy.forMonotonousTimestamps() 基于时间戳单调递增的 watermark
    // WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100) 固定延迟策略
    val sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    sourceStream
      .flatMap(new RichFlatMapFunction[String, Behavior] {

        var jsonParse: JsonParser = _

        override def open(parameters: Configuration): Unit = {
          LOG.info("open flatMap")
          jsonParse = new JsonParser
        }

        override def flatMap(element: String, out: Collector[Behavior]): Unit = {

          var jsonObject: JsonObject = null
          try {
            jsonObject = jsonParse.parse(element).getAsJsonObject
          } catch {
            case _ => {
//              LOG.warn("parse json error: ", element)
              return
            }
          }
          var userId: String = null
          if (jsonObject.has("user_id")) {
            userId = jsonObject.get("user_id").getAsString
          }
          var url: String = null
          if (jsonObject.has("behavior")) {
            url = jsonObject.get("behavior").getAsString
          }
          var ts: Long = -1
          if (jsonObject.has("ts")) {
            val tmp = jsonObject.get("ts").getAsString
            ts = DateTimeUtil.parse(tmp).getTime
          }

          val behavior = new Behavior(userId, url, ts)
          val record = new StreamElement[Behavior](behavior, System.currentTimeMillis())

          out.collect(behavior)
        }
      })
      .name("flatMap")
      .uid("flatMap")
      // timestamp & watermart
      .assignTimestampsAndWatermarks(WatermarkStrategy
        // 固定延迟时间
        .forBoundedOutOfOrderness(Duration.ofMinutes(1))
        // timestamp
        .withTimestampAssigner(TimestampAssignerSupplier.of(new SerializableTimestampAssigner[Behavior] {
          override def extractTimestamp(element: Behavior, recordTimestamp: Long): Long =
            element.getTs
        }))
      )
      // 翻滚窗口
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
      .allowedLateness(Time.minutes(1))
      // 窗口函数
      .process(new ProcessAllWindowFunction[Behavior, String, TimeWindow]() {
        override def process(context: Context, elements: Iterable[Behavior], out: Collector[String]): Unit = {
          val window = context.window
          val windowStart = DateTimeUtil.formatMillis(window.getStart, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)
          val windowEnd = DateTimeUtil.formatMillis(window.getStart, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)

          var count = 0
          val list = new util.ArrayList[String]()

          elements.foreach(b => {
            count += 1
            list.add(DateTimeUtil.formatMillis(b.getTs, DateTimeUtil.YYYY_MM_DD_HH_MM_SS))
          })

          out.collect(windowStart + " - " + windowEnd + " : " + count + ", element : " + util.Arrays.toString(list.toArray))
        }
      }
      )
      .print()

    env.execute("WhenWindowFire")
  }

}
