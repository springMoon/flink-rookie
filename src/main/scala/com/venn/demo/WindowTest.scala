package com.venn.demo

import java.time.Duration
import com.google.gson.{JsonObject, JsonParser}
import com.venn.entity.{Behavior, StreamElement}
import com.venn.util.DateTimeUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssignerSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.regex.Pattern

object WindowTest {

  val LOG = LoggerFactory.getLogger("DayWindow")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val topic = "deepexi_*"


    val bootstrapServer = "dcmp12:9092"
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setTopicPattern(Pattern.compile(topic))
      .setGroupId("day_window")
      //      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    // WatermarkStrategy.forMonotonousTimestamps() 基于时间戳单调递增的 watermark
    // WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100) 固定延迟策略
    val sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    val filter = sourceStream
      .disableChaining()
      .flatMap(new RichFlatMapFunction[String, StreamElement[String]] {

        var jsonParse: JsonParser = _

        override def open(parameters: Configuration): Unit = {
          LOG.info("open flatMap")
          jsonParse = new JsonParser
        }

        override def flatMap(element: String, out: Collector[StreamElement[String]]): Unit = {

          val ele = new StreamElement[String](element, System.currentTimeMillis())

          out.collect(ele)
        }
      })
      .name("flatMap")
      .uid("flatMap")
      .disableChaining()
      .filter(new RichFilterFunction[StreamElement[String]] {
        var INTERVAL: Long = _

        override def open(parameters: Configuration): Unit = {
          INTERVAL = 10 * 60 * 1000
        }

        override def filter(element: StreamElement[String]): Boolean = {
          true
        }
      })
      .name("filter")
      .uid("filter")
//      .disableChaining()
//
//      .assignTimestampsAndWatermarks(WatermarkStrategy
//        // 固定延迟时间
//        .forBoundedOutOfOrderness(Duration.ofMillis(1))
//        //      .forMonotonousTimestamps()
//        .withTimestampAssigner(TimestampAssignerSupplier.of(new SerializableTimestampAssigner[StreamElement[Behavior]] {
//          override def extractTimestamp(element: StreamElement[String], recordTimestamp: Long): Long =
//            element.getData.getTs
//        }))
//        .withIdleness(Duration.ofSeconds(100))
//      )
      //      .assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarksAdapter[StreamElement[Behavior]])
      .process(new ProcessFunction[StreamElement[String], String]() {
        override def processElement(element: StreamElement[String], ctx: ProcessFunction[StreamElement[String], String]#Context, out: Collector[String]): Unit = {
          val ts = ctx.timestamp()
          val watermark = ctx.timerService().currentWatermark()

          println("element : " + element)

          out.collect(element.getData)
        }
      })
    //      .print

    val sink = new FlinkKafkaProducer[String](bootstrapServer,  "window_test_sink", new SimpleStringSchema())

    filter.addSink(sink)

    env.execute("DayWindow")
  }

}
