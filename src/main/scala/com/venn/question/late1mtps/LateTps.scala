package com.venn.question.late1mtps

import com.google.gson.JsonParser
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.source.TumblingEventTimeWindows
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

object LateTps {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val topic = "user_log"
    val bootstrapServer = "localhost:9092"
    // second
    val windowSize: Int = 10 * 60
    val intervalSize: Int = 10

    val kafkaSource = KafkaSource
      .builder[KafkaSimpleStringRecord]()
      .setTopics(topic)
      .setBootstrapServers(bootstrapServer)
      .setGroupId("late_tps")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .build()

    val source = env
      .fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "kafkaSource")

    val stream = source
      .map(new RichMapFunction[KafkaSimpleStringRecord, (String, Long)] {

        var jsonParse: JsonParser = _

        override def open(parameters: Configuration): Unit = {
          jsonParse = new JsonParser

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
      //      todo timestamp and watermark
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
          override def extractTimestamp(t: (String, Long), l: Long): Long = {
            t._2
          }
        })
        .withIdleness(Duration.ofMinutes(1))
      )


    // windowSize minute, export every 1 minute tps
//    val process10m = stream
//      .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
//      .process(new LateTpsProcessAllWindowFunction(windowSize, 60))
//      .print("10m")

//    // windowSize minute, export every 1 minute tps
    val process10s = stream
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .process(new LateTpsSecondProcessAllWindowFunction(windowSize , intervalSize))

    process10s.print("10s")

    val tag = new OutputTag[String]("size")
    val side = process10s.getSideOutput(tag)

    val kafkaSink = KafkaSink.builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(topic +"_side_sink")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()
//      .setTopic(topic + "_side_sink")
//      .setValueSerializationSchema(new SimpleStringSchema())
//      .build())

    side.sinkTo(kafkaSink)


    env.execute("LateTps")
  }

}
