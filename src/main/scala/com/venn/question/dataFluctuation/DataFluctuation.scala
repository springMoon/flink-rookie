package com.venn.question.dataFluctuation

import com.google.gson.JsonParser
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.util.{CheckpointUtil, DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/*
  计算数据波动
 */
object DataFluctuation {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val checkpointInterval = 60 * 1000
    val checkpointTimeOut = 2 * checkpointInterval
    val checkPointPath = "hdfs:///tmp/flink/checkpoint"
    val bootstrapServer = "localhost:9092"
    val topic = "user_log"

    // set checkpoint
    CheckpointUtil.setCheckpoint(env, "FileSystem", checkPointPath, checkpointInterval, checkpointTimeOut)

    val kafkaSource = KafkaSource
      .builder[KafkaSimpleStringRecord]()
      .setBootstrapServers(bootstrapServer)
      .setTopics(topic)
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema)
      .setStartingOffsets(OffsetsInitializer.latest())
      .build()

    val source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    val stream = source.map(new RichMapFunction[KafkaSimpleStringRecord, (String, Double, Long)] {
      var jsonParser: JsonParser = _

      override def open(parameters: Configuration): Unit = {
        jsonParser = new JsonParser
      }

      override def map(element: KafkaSimpleStringRecord): (String, Double, Long) = {

        val json = jsonParser.parse(element.getValue).getAsJsonObject

        val item = json.get("item").getAsString
        val price = json.get("price").getAsDouble
        val tsStr = json.get("ts").getAsString
        val ts = DateTimeUtil.parse(tsStr).getTime

        (item, price, ts)
      }
    })
      .name("map")
      .uid("map")

    stream
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forGenerator((_: WatermarkGeneratorSupplier.Context) => {
          new WatermarkGenerator[(String,Double, Long)] {
            var current = 0l
            override def onEvent(t: (String, Double, Long), l: Long, watermarkOutput: WatermarkOutput): Unit = {
              if(t._3 > current){
                current = t._3
                watermarkOutput.emitWatermark(new Watermark(current))
              }

            }

            override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
              //
            }
          }

        }))



  }

}
