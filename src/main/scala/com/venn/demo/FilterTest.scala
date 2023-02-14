package com.venn.demo

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import java.util.regex.Pattern
import scala.util.Random

object FilterTest {


  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val topic = "filter_test"

    val random = new Random();
    print(random.nextString(16))

    val bootstrapServer = "localhost:9092"
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setTopicPattern(Pattern.compile(topic))
      .setGroupId("day_window")
      //      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()


    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSoruce1")
      .filter(str => str.equals("abc"))
      .print(">>")

    env.execute("exec")


  }

}
