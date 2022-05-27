package com.venn.question.dayWindow

import com.google.gson.{JsonObject, JsonParser}
import com.venn.common.Common
import com.venn.entity.StreamElement
import com.venn.question.retention.UserLog
import com.venn.source.TumblingEventTimeWindows
import com.venn.util.DateTimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssignerSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.time.Duration

/**
 * flink day window, full data not exists
 *
 * 用flinksql求每个app_code的pv、uv，1小时的窗口，30秒刷新一次结果，
 * 要求每个小时每个app_code都有统计结果（用0填充）。填充这个问题没弄好，
 * 本来准备把24小时与所有app_code进行full join，
 * 再left join流表，但这样会提交不上checkpoint。
 * 有没有大佬支个招，版本flink 1.11.1
 *
 */
object DayWindowFullData {

  val LOG = LoggerFactory.getLogger("DayWindowFullData")

  def main(args: Array[String]): Unit = {

    val topic = "user_log"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(Common.BROKER_LIST)
      .setTopics(topic)
      .setGroupId("day_window_full_data")
      //      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build();

    val sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    sourceStream
      .flatMap(new RichFlatMapFunction[String, StreamElement[UserLog]] {

        var jsonParse: JsonParser = _

        override def open(parameters: Configuration): Unit = {
          LOG.info("open flatMap")
          jsonParse = new JsonParser
        }

        override def flatMap(element: String, out: Collector[StreamElement[UserLog]]): Unit = {

          var jsonObject: JsonObject = null
          try {
            jsonObject = jsonParse.parse(element).getAsJsonObject
          } catch {
            case _ => {
              LOG.warn("parse json error: ", element)
              return
            }
          }
          var userId: String = null
          if (jsonObject.has("user_id")) {
            userId = jsonObject.get("user_id").getAsString
          }
          var itemId: Int = 0
          if (jsonObject.has("item_id")) {
            itemId = jsonObject.get("item_id").getAsInt
          }
          var categoryId: Int = 0
          if (jsonObject.has("category_id")) {
            categoryId = jsonObject.get("category_id").getAsInt
          }
          var behavior: String = null
          if (jsonObject.has("behavior")) {
            behavior = jsonObject.get("behavior").getAsString
          }
          var ts: Long = -1
          var tmp: String = ""
          if (jsonObject.has("ts")) {
            tmp = jsonObject.get("ts").getAsString
            ts = DateTimeUtil.parse(tmp).getTime
          }

          val userLog = new UserLog(userId, itemId, categoryId, behavior, tmp, ts)
          val record = new StreamElement[UserLog](userLog, System.currentTimeMillis())

          out.collect(record)
        }
      })
      .name("flatMap")
      .uid("flatMap")
      .filter(new RichFilterFunction[StreamElement[UserLog]] {
        var INTERVAL: Long = _

        override def open(parameters: Configuration): Unit = {
          INTERVAL = 10 * 60 * 1000;
        }

        override def filter(element: StreamElement[UserLog]): Boolean = {
          // user_id 不为空，长度大于 8 位
          if (StringUtils.isEmpty(element.data.userId) || element.getData.userId.length > 8) {
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
        .withTimestampAssigner(TimestampAssignerSupplier.of(new SerializableTimestampAssigner[StreamElement[UserLog]] {
          override def extractTimestamp(element: StreamElement[UserLog], recordTimestamp: Long): Long =
            element.getData.tsLong
        }))
        .withIdleness(Duration.ofSeconds(100))
        // todo 固定标记的 watermark
        //        .createWatermarkGenerator()

      )
      //      .keyBy(be => be.data.behavior)
      //      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      //      .process(new ProcessWindowFunction[StreamElement[UserLog], String, String, TimeWindow] {
      //        override def process(key: String, context: Context, elements: Iterable[StreamElement[UserLog]], out: Collector[String]): Unit = {
      //          var coun = 0
      //          elements.foreach(e => {coun += 1})
      //
      //          out.collect(key + "," + coun)
      //        }
      //      })
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
      .process(new ProcessAllWindowFunction[StreamElement[UserLog], String, TimeWindow] {

        override def process(context: Context, elements: Iterable[StreamElement[UserLog]], out: Collector[String]): Unit = {

          val time = DateTimeUtil.formatMillis(context.window.getStart, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)

          var pv: Int = 0
          var buy: Int = 0
          var car: Int = 0
          var fav: Int = 0
          elements.foreach(element => {
//            val behavior = element.data.behavior
            element.data.behavior match {
              case "pv" => pv += 1
              case "buy" => buy += 1
              case "car" => car += 1
              case "fav" => fav += 1
              case _ =>
            }

          })
          out.collect(time + ",pv," + pv)
          out.collect(time + ",buy," + buy)
          out.collect(time + ",car," + car)
          out.collect(time + ",fav," + fav)
        }
      })
      .print()


    env.execute("DayWindowFullData")
  }

}
