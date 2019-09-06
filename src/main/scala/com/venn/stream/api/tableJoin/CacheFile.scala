package com.venn.stream.api.tableJoin

import java.io.File
import java.text.SimpleDateFormat

import com.venn.common.Common
import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.io.Source

/**
  * stream join read config from cache file
  *   register at job start, never change again
  */
object CacheFile {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if ("/".equals(File.separator)) {
      val backend = new FsStateBackend(Common.CHECK_POINT_DATA_DIR, true)
      env.setStateBackend(backend)
      env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE)
      env.registerCachedFile("/opt/flink1.7/data/tablejoin.txt", "tablejoin.txt")
    } else {
      env.setMaxParallelism(1)
      env.setParallelism(1)
      // file and register name
      env.registerCachedFile("C:\\Users\\venn\\git\\venn\\flinkDemo\\src\\main\\resources\\data\\tablejoin.txt", "tablejoin.txt")
    }
    // cache table


    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val source = new FlinkKafkaConsumer[ObjectNode]("table_join", new JsonNodeDeserializationSchema, Common.getProp)


    env.addSource(source)
      .map(json => {

        val id = json.get("id").asText()
        val phone = json.get("phone").asText()

        Tuple2(id, phone)
      })
      .map(new RichMapFunction[(String, String), String] {

        var cache = Map("" -> "")

        override def open(parameters: Configuration): Unit = {

          // read cache file
          val file = getRuntimeContext.getDistributedCache.getFile("tablejoin.txt")
          if (file.canRead) {
            val context = Source.fromFile(file, "utf-8").getLines().toArray

           context.foreach(line => {
             val tmp = line.split(",")
             cache += (tmp(0) -> tmp(1))
           })
          }
        }

        override def map(value: (String, String)): String = {
          val name = cache.get(value._1)

          value._1 + "," + value._2 + "," + cache.get(value._1)
        }

      })
      .print()

    env.execute("cacheFile")

  }

}
