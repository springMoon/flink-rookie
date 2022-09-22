package com.venn.stream.api.trigger

import java.io.File
import java.text.SimpleDateFormat
import com.venn.common.Common
import com.venn.util.CheckpointUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * for test CountAndContinuousProcessTimeTrigger
 *
 */
object ProcessWindowDemoForTrigger {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    if ("\\".equals(File.pathSeparator)) {
      //      val rock = new RocksDBStateBackend(Common.CHECK_POINT_DATA_DIR)
      //      env.setStateBackend(rock)
      // checkpoint interval
      //      env.enableCheckpointing(10000)
      CheckpointUtil.setCheckpoint(env, "rocksdb", Common.CHECK_POINT_DATA_DIR, 10)
    }

    val topic = "current_day"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val kafkaSource = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), Common.getProp)
    val stream = env.addSource(kafkaSource)
      .map(s => {
        s
      })
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .trigger(CountAndTimeTrigger.of(10, Time.seconds(10)))
      .process(new ProcessAllWindowFunction[String, String, TimeWindow] {

        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {

          var count = 0

          elements.iterator.foreach(s => {
            count += 1
          })
          logger.info("this trigger have : {} item", count)
        }

      })

    // execute job
    env.execute(this.getClass.getName)
  }

}


