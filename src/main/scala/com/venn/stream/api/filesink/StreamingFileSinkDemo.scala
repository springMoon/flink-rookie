package com.venn.stream.api.filesink

import java.io.File
import java.text.SimpleDateFormat

import com.venn.common.Common
import org.apache.flink.api.common.serialization.{BulkWriter, SimpleStringEncoder}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object StreamingFileSinkDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if ("/".equals(File.separator)) {
      val backend = new FsStateBackend(Common.CHECK_POINT_DATA_DIR, true)
      env.setStateBackend(backend)
      env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE)
    } else {
      env.setMaxParallelism(1)
      env.setParallelism(1)
    }

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val source = new FlinkKafkaConsumer[ObjectNode]("roll_file_sink", new JsonNodeDeserializationSchema, Common.getProp)
    // row format
    val sinkRow = StreamingFileSink
      .forRowFormat(new Path("D:\\idea_out\\rollfilesink"), new SimpleStringEncoder[ObjectNode]("UTF-8"))
      .withBucketAssigner(new DayBucketAssigner)
      .withBucketCheckInterval(60 * 60 * 1000l) // 1 hour
      .build()

    // use define BulkWriterFactory and DayBucketAssinger
    val sinkBuck = StreamingFileSink
      .forBulkFormat(new Path("D:\\idea_out\\rollfilesink"), new DayBulkWriterFactory)
      .withBucketAssigner(new DayBucketAssigner())
      .withBucketCheckInterval(60 * 60 * 1000l) // 1 hour
      .build()


    env.addSource(source)
      .assignAscendingTimestamps(json => {
        sdf.parse(json.get("date").asText()).getTime
      })
      .map(json => {
//        json.get("date") + "-" + json.toString
        json
      })
      .addSink(sinkBuck)

    env.execute("StreamingFileSink")
  }

}
