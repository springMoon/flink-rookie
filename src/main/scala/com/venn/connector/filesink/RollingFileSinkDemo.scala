package com.venn.connector.filesink

import java.io.File
import java.text.SimpleDateFormat

import com.venn.common.Common
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

/**
  * 使用BucketingSink 实现 根据‘数据’自定义输出目录
  */
object RollingFileSinkDemo {

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

    /**
      * 这里有个问题，因为重写了BasePathBucketer，自定义了输出文件，
      * 所有会同时打开多个输出文件，带来文件刷新的问题，在当前文件写完后(这里的表现是：当天
      * 的数据以及全部流过，下一天的文件以及开始写了)，会发现
      * 当天的文件中的数据不全，因为数据还没有全部刷到文件，这个时候下一个文件
      * 又开始写了，会发现上一个文件还没刷完。
      *
      * 猜想：每个文件都有个输出缓冲，上一个文件最后一点数据还在缓冲区，下一个文件
      * 又使用新的缓冲区，没办法刷到上一个文件的数据，只有等缓冲区数据满、超时一类的操作触发刷写 ？？
      *
      * 源码BucketingSink.closePartFilesByTime
      *   默认每60秒或大于滚动时间间隔（batchRolloverInterval）（系统时间） 将当前park文件，
      *   将状态从 in-process 修改为 pending，随后
      *   关闭当前的part 文件，数据刷到磁盘
      *
      */
    val sink = new BucketingSink[String]("D:\\idea_out\\rollfilesink")
    sink.setBucketer(new DayBasePathBucketer)
    sink.setWriter(new StringWriter[String])
    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //    sink.setBatchRolloverInterval(24 * 60 * 60 * 1000) // this is 24 hour
//    sink.setInProgressPrefix("inProcessPre")
//    sink.setPendingPrefix("pendingpre")
//    sink.setPartPrefix("partPre")

    env.addSource(source)
      .assignAscendingTimestamps(json => {
        sdf.parse(json.get("date").asText()).getTime
      })
      .map(json => {
        json.get("date") + "-" + json.toString
      })
      .addSink(sink)

    env.execute("rollingFileSink")
  }

}
