package com.venn.question.late1mtps

import com.google.gson.JsonParser
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.source.TumblingEventTimeWindows
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
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

object LateTpsWatermark {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val topic = "user_log"
    val bootstrapServer = "localhost:9092"
    // window size second
    val windowSize: Int = 10 * 60
    // calculate tps interval
    val intervalSize: Int = 10

    // kafka source for read data
    val kafkaSource = KafkaSource
      .builder[KafkaSimpleStringRecord]()
      .setTopics(topic)
      .setBootstrapServers(bootstrapServer)
      .setGroupId("late_tps")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .build()

    // add source
    val source = env
      .fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "kafkaSource")

    // parse data, only get (user_id, ts)
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
      // set timestamp and watermark
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forGenerator((context: WatermarkGeneratorSupplier.Context) => {
          new WatermarkGenerator[(String, Long)] {
            var timestamp: Long = _

            override def onEvent(t: (String, Long), l: Long, watermarkOutput: WatermarkOutput): Unit = {
              if (t._2 > timestamp) {
                timestamp = t._2
                watermarkOutput.emitWatermark(new Watermark(timestamp))
              }
            }

            override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
              //                watermarkOutput.emitWatermark(new Watermark(ts))
            }
          }
        }
        )
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(t: (String, Long), l: Long): Long = t._2
          })
          .withIdleness(Duration.ofSeconds(5))
      )
    // common watermark generator
    //      .assignTimestampsAndWatermarks(WatermarkStrategy
    //        .forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5))
    //        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
    //          override def extractTimestamp(t: (String, Long), l: Long): Long = {
    //            t._2
    //          }
    //        })
    //        // idle 1 minute
    //        .withIdleness(Duration.ofMinutes(1))
    //      )


    // windowSize 10 minute, export every 1 minute tps
    val process10m = stream
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .process(new FixedLateTpsProcessAllWindowFunction(windowSize, 60))
      .print("10m")

    //    // windowSize minute, export every 1 minute tps
    val process10s = stream
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .process(new AdjustLateTpsProcessAllWindowFunction(windowSize, intervalSize))

    process10s.print("10s")

    val tag = new OutputTag[String]("size")
    val side = process10s.getSideOutput(tag)

    // side tmp result to kafka
    val kafkaSink = KafkaSink.builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(topic + "_side_sink")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()

    // add sink
    side.sinkTo(kafkaSink)

    // execute task
    env.execute("LateTps")
  }

}
