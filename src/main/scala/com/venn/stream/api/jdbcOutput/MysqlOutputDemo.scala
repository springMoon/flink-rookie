package com.venn.stream.api.jdbcOutput

import java.io.File

import com.venn.common.Common
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * 侧边输出：This operation can be useful when you want to split a stream of data
  */
object MysqlOutputDemo {

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

    val source = new FlinkKafkaConsumer[String]("mysql_output", new SimpleStringSchema, Common.getProp)
    source.setStartFromLatest()
    env.addSource(source)
        .map(li => {
          val tmp = li.split(",")
          new User(tmp(0), tmp(1), tmp(2)toInt, tmp(3))
        })
//        .addSink(new MysqlSink1)
      .writeUsingOutputFormat(new MysqlSink1)

    env.execute("msqlOutput")
  }

}
