package com.venn.question.dayWindow

import java.time.Duration

import com.google.gson.{JsonObject, JsonParser}
import com.venn.common.Common
import com.venn.entity.{Behavior, StreamElement}
import com.venn.util.DateTimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssignerSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * flink compute day/werk/month pv/uv
 */
object DayWindow {

  val LOG = LoggerFactory.getLogger("DayWindow")

  def main(args: Array[String]): Unit = {

    val topic = "user_log"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(Common.BROKER_LIST)
      .setTopics(topic)
      .setGroupId("day_window")
      //      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build();

    // WatermarkStrategy.forMonotonousTimestamps() 基于时间戳单调递增的 watermark
    // WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100) 固定延迟策略
    val sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    sourceStream
      .flatMap(new RichFlatMapFunction[String, StreamElement[Behavior]] {

        var jsonParse: JsonParser = _

        override def open(parameters: Configuration): Unit = {
          LOG.info("open flatMap")
          jsonParse = new JsonParser
        }

        override def flatMap(element: String, out: Collector[StreamElement[Behavior]]): Unit = {

          var jsonObject: JsonObject = null
          try {
            jsonObject = jsonParse.parse(element).getAsJsonObject
          } catch {
            case _ => {
              LOG.warn("parse json error: ", element)
            }
          }
          var userId: String = null
          if (jsonObject.has("user_id")) {
            userId = jsonObject.get("user_id").getAsString
          }
          var url: String = null
          if (jsonObject.has("url")) {
            url = jsonObject.get("url").getAsString
          }
          var ts: Long = -1
          if (jsonObject.has("ts")) {
            val tmp = jsonObject.get("ts").getAsString
            ts = DateTimeUtil.parse(tmp).getTime
          }

          val behavior = new Behavior(userId, url, ts)
          val record = new StreamElement[Behavior](behavior, System.currentTimeMillis())

          out.collect(record)
        }
      })
      .name("flatMap")
      .uid("flatMap")
      .filter(new RichFilterFunction[StreamElement[Behavior]] {
        var INTERVAL: Long = _

        override def open(parameters: Configuration): Unit = {
          INTERVAL = 10 * 60 * 1000;
        }

        override def filter(element: StreamElement[Behavior]): Boolean = {
          // user_id 不为空，长度大于 8 位
          if (StringUtils.isEmpty(element.data.getUserId) || element.getData.getUserId.length > 8) {
            return false
          }
          // url ignore
          // latest 10 minute & less than current time
          val current = System.currentTimeMillis()
          if (current - element.data.getTs <= INTERVAL && element.data.getTs <= current) {
            return false
          }
          true
        }
      })
      .name("filter")
      .uid("filter")
      .assignTimestampsAndWatermarks(WatermarkStrategy
        // 固定延迟时间
        .forBoundedOutOfOrderness(Duration.ofSeconds(1))
        .withTimestampAssigner(TimestampAssignerSupplier.of(new SerializableTimestampAssigner[StreamElement[Behavior]] {
          override def extractTimestamp(element: StreamElement[Behavior], recordTimestamp: Long): Long =
            element.getData.getTs
        }))
        .withIdleness(Duration.ofSeconds(100))
        // todo 固定标记的 watermark
        //        .createWatermarkGenerator()

      )
      //      .assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarksAdapter[StreamElement[Behavior]])
      .process(new ProcessFunction[StreamElement[Behavior], StreamElement[Behavior]]() {
        override def processElement(element: StreamElement[Behavior], ctx: ProcessFunction[StreamElement[Behavior], StreamElement[Behavior]]#Context, out: Collector[StreamElement[Behavior]]): Unit = {
          val ts = ctx.timestamp();
          val watermark = ctx.timerService().currentWatermark();

          println("ts: " + ts + ", watermark : " + watermark)

        }
      })


    env.execute("DayWindow")
  }

}
