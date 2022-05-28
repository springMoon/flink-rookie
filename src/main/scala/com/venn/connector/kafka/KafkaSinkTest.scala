package com.venn.connector.kafka

import com.venn.common.Common
import com.venn.question.retention.RetentionAnalyze.bootstrapServer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 *
 * 请教个问题哈，sink 到 kafka，采用默认的分区器，是不是每个并行度都会与kafka的partition维护一个连接

比如 10 个并行度，3个 partition，那么维护的连接数总共为 10*3 个  ？  是的

还是一个taskManager建立一个生产者 一个生产者对应多个分区

一个taskManager里面多个slot共享一个生产者？ no
 */
object KafkaSinkTest {

  val LOG = LoggerFactory.getLogger("KafkaSinkTest")

  def main(args: Array[String]): Unit = {

    val topic = "user_log"
    val sinkTopic = "user_log_sink_1"

    // env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // global parllelism
    val parallelism = 4
    env.setParallelism(parallelism)

    // kafka source
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(Common.BROKER_LIST)
      .setTopics(topic)
      .setGroupId("KafkaSinkTest")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build();

    // kafka sink
    val kafkaSink = KafkaSink
      .builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setKafkaProducerConfig(Common.getProp)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(sinkTopic)
        // 不指定 key 的序列号器，key 会为 空
//        .setKeySerializationSchema(new SimpleStringSchema())
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()


    // add source，读取数据
    val sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    // map, add current subtask index
    val mapStream = sourceStream
      // rebalance data to all parallelisn
      .rebalance
      .flatMap(new RichFlatMapFunction[String, String] {
        override def flatMap(element: String, out: Collector[String]): Unit = {
          val parallelism = getRuntimeContext.getIndexOfThisSubtask
          out.collect(parallelism + "," + element)

        }
      })
      .name("flatMap")
      .uid("flatMap")

    // sink to kafka, new api
//    mapStream.sinkTo(kafkaSink)

    // sink to kafka, old api
        val kafkaProducer = new FlinkKafkaProducer[String](bootstrapServer,sinkTopic, new SimpleStringSchema())
        mapStream.addSink(kafkaProducer)
          .setParallelism(parallelism)

    env.execute("KafkaSinkTest")
  }

}
