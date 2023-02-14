package com.venn.question.late1mtps

import com.google.gson.JsonParser
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.source.TumblingEventTimeWindows
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime.{TimestampAssignerSupplier, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec.DefaultWatermarkGeneratorSupplier.DefaultWatermarkGenerator

import java.time.Duration
import java.util.Date

object LateTps {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val topic = "user_log"
    val bootstrapServer = "localhost:9092"

    val source = KafkaSource
      .builder[KafkaSimpleStringRecord]()
      .setTopics(topic)
      .setBootstrapServers(bootstrapServer)
      .setGroupId("late_tps")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .build()

    env
      .fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "kafkaSource")
      .map(new RichMapFunction[KafkaSimpleStringRecord, String] {

        var jsonParse:JsonParser = _

        override def open(parameters: Configuration): Unit = {
          jsonParse = new JsonParser;

        }

        override def map(element: KafkaSimpleStringRecord): (String, Long) = {

          val json = jsonParse.parse(element.getValue).getAsJsonObject

          val tsStr = json.get("ts").getAsString
          val ts = DateTimeUtil.parse(tsStr).getTime
          val userId = json.get("user_id").getAsString

          (userId, ts)

        }

        override def close(): Unit = {
          jsonParse = null

        }
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((String, Long) -> _.2)
      )
      .windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
      .process(new LateTpsProcessAllWindowFunction[KafkaSimpleStringRecord, String, TimeWindow](9))


  }

}
