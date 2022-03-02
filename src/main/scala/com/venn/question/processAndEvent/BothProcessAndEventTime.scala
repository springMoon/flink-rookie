package com.venn.question.processAndEvent

import com.google.gson.JsonParser
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.question.retention.UserLog
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

object BothProcessAndEventTime {

  val LOG = LoggerFactory.getLogger("BothProcessAndEventTime")
  val bootstrapServer = "localhost:9092"
  val topic = "user_log"
  val sinkTopic = "user_log_sink"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    // source
    val kafkaSource = KafkaSource
      .builder[KafkaSimpleStringRecord]()
      .setBootstrapServers(bootstrapServer)
      .setGroupId("ra")
      .setTopics(topic)
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .build()

    // 不使用 IngestionTime 指定 watermark，后续从数据中提取时间戳和 watermark
    val source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "source")
      .name("source")
      .uid("source")

    val stream = source.flatMap(new RichFlatMapFunction[KafkaSimpleStringRecord, UserLog] {
      var jsonParse: JsonParser = _

      override def open(parameters: Configuration): Unit = {
        jsonParse = new JsonParser
      }

      // parse json to UserLog
      override def flatMap(element: KafkaSimpleStringRecord, out: Collector[UserLog]): Unit = {
        try {
          val jsonObject = jsonParse.parse(element.getValue).getAsJsonObject
          val userId = jsonObject.get("user_id").getAsString
          val categoryId = jsonObject.get("category_id").getAsInt
          val itemId = jsonObject.get("item_id").getAsInt
          val behavior = jsonObject.get("behavior").getAsString
          val ts = jsonObject.get("ts").getAsString
          val tsLong = DateTimeUtil.parse(ts).getTime
          val userLog = UserLog(userId, categoryId, itemId, behavior, ts, tsLong)

          out.collect(userLog)
        } catch {
          case _ =>
            LOG.warn("parse json error : " + element.getValue)
        }

      }
    })
      .name("map")
      .uid("map")
      // default is IngestionTime, kafka source will add timestamp to StreamRecord,
      // if not set assignAscendingTimestamps, use StreamRecord' timestamp, so is ingestion time
//            .assignAscendingTimestamps(userLog => userLog.tsLong)
            // create watermark by all elements
//      .assignTimestampsAndWatermarks(new WatermarkStrategy[UserLog] {
//        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[UserLog] = {
//          new WatermarkGenerator[UserLog] {
//            var watermark: Watermark = new Watermark(Long.MinValue)
//
//            override def onEvent(element: UserLog, eventTimestamp: Long, output: WatermarkOutput): Unit = {
//              watermark = new Watermark(element.tsLong - 1)
//              output.emitWatermark(watermark)
//            }
//
//            override def onPeriodicEmit(output: WatermarkOutput): Unit = {
//              output.emitWatermark(watermark)
//            }
//          }
//        }
//      })
      // key all data to one key
      .keyBy(new KeySelector[UserLog, String] {
        override def getKey(element: UserLog): String = {
          "1"
        }
      })


    // event
    stream
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      //      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .process(new SimpleProcessFunction("eventTime"))
      .name("event")
      .uid("event")
      .disableChaining()
      .print()

    // process
    stream
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      .process(new SimpleProcessFunction("processTime"))
      .name("process")
      .uid("process")
      .disableChaining()
      .print()


    env.execute("BothProcessAndEventTime")

  }

}
