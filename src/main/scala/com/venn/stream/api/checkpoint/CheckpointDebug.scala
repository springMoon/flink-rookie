package com.venn.stream.api.checkpoint

import com.venn.common.Common
import com.venn.demo.CustomerSource
import com.venn.source.TumblingEventTimeWindows
import com.venn.util.CheckpointUtil
import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * for debug checkpoint
 */
object CheckpointDebug {
  val LOG = LoggerFactory.getLogger("CheckpointDebug")

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    CheckpointUtil.setCheckpoint(env, "rocksdb", Common.CHECK_POINT_DATA_DIR, 60)

    val prop = Common.getProp()
    val kafkaSource1 = new FlinkKafkaConsumer[String]("source_1", new SimpleStringSchema(), prop)
    val kafkaSource2 = new FlinkKafkaConsumer[String]("source_2", new SimpleStringSchema(), prop)
    val source1 = env.addSource(kafkaSource1)
      .name("source1")

    val source2 = env.addSource(kafkaSource2)
      .name("source2")
    val map1 = source1.map(item => {
      val arr = item.split(",")
      ("map_1", arr(0).toLong, arr(1).toLong)
    })
      .name("map1")

    val map2 = source2.map(item => {
      val arr = item.split(",")
      ("map_1", arr(0).toLong, arr(1).toLong)
    })
      .name("map2")

    val join = map1.join(map2)
      .where(_._2)
      .equalTo(_._2)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply(new RichFlatJoinFunction[(String, Long, Long), (String, Long, Long), (String, String, Long, Long)] {

        override def open(parameters: Configuration): Unit = {
          LOG.info("RichFlatJoinFunction open")

        }

        // join
        override def join(first: (String, Long, Long), second: (String, Long, Long), out: Collector[(String, String, Long, Long)]): Unit = {


          out.collect((first._1, second._1, first._2, first._3))

        }

        override def close(): Unit = {
          LOG.info("RichFlatJoinFunction close")

        }
      })
      .name("join")


    val kafkaSink = new FlinkKafkaProducer[String]("localhost:9092", "checkpoint_debug", new SimpleStringSchema())
    val sink = join.map(item => {
      item._1 + "," + item._2 + "," + item._3 + "," + item._4
    })
      .name("joinFormat")
      .addSink(kafkaSink)
      .name("sink")


    env.execute("checkpointDebug")

  }

}
