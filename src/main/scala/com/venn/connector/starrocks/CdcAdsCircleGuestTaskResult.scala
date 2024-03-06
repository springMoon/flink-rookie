package com.venn.connector.starrocks

import com.google.gson.{JsonObject, JsonParser}
import com.starrocks.connector.flink.StarRocksSink
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions
import com.venn.connector.cdc.DdlDebeziumDeserializationSchema
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.sys.env

/**
 * @Classname CdcAdsCircleGuestTaskResult
 * @Description TODO
 * @Date 2024/3/6
 * @Created by venn
 */
object CdcAdsCircleGuestTaskResult {

  val ip = "10.20.131.192"
  val jdbcPort = "9030"
  val httpPort = "18030"
  val user = "root"
  val pass = "showyu123"
  var batch = 64000
  var interval = 5

  var sourceId = "rm-2ze82f881xft670dm.mysql.rds.aliyuncs.com"
  var sourceUser = "deepexi"
  var sourcePass = "mvqRxerKIgQadEO1M74"


  def main(args: Array[String]): Unit = {

    if (args.length >= 3) {
      sourceId = args(0)
      sourceUser = args(1)
      sourcePass = args(2)
    }


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.disableOperatorChaining()

    // cdc source
    val source = MySqlSource.builder[String]()
      .hostname("rm-2ze82f881xft670dm.mysql.rds.aliyuncs.com")
      .port(3306)
      .username("deepexi")
      .password("mvqRxerKIgQadEO1M74")
      .databaseList("showyu-digital-operation")
      .tableList("showyu-digital-operation.ads_circle_guest_task_result")
      .serverTimeZone("Asia/Shanghai")
      // 包含 schema change
      .includeSchemaChanges(true)
      //      .startupOptions(StartupOptions.latest())
      .startupOptions(StartupOptions.initial())
      .deserializer(new DdlDebeziumDeserializationSchema("", 3306))
      .build()


    env.setParallelism(1)


    val sink = StarRocksSink.sink(
      // the sink options
      StarRocksSinkOptions.builder()
        .withProperty("jdbc-url", "jdbc:mysql://" + ip + ":" + jdbcPort)
        .withProperty("load-url", ip + ":" + httpPort)
        .withProperty("username", user)
        .withProperty("password", pass)
        .withProperty("database-name", "test")
        .withProperty("table-name", "ads_circle_guest_task_result")
        // 自 2.4 版本，支持更新主键模型中的部分列。您可以通过以下两个属性指定需要更新的列。
        // .withProperty("sink.properties.partial_update", "true")
        // .withProperty("sink.properties.columns", "k1,k2,k3")
        .withProperty("sink.properties.format", "json")
        .withProperty("sink.properties.strip_outer_array", "true")
        //        .withProperty("sink.properties.row_delimiter", ROW_SEP)
        //        .withProperty("sink.properties.column_separator", COL_SEP)
        // 设置并行度，多并行度情况下需要考虑如何保证数据有序性
        .withProperty("sink.parallelism", "1")
        .withProperty("sink.version", "v1")
        .withProperty("sink.buffer-flush.max-rows", "" + batch)
        .withProperty("sink.buffer-flush.interval-ms", "" + (interval * 1000))
        .build())
    val map = env.fromSource(source, WatermarkStrategy.noWatermarks[String](), "cdc")
      .map(new RichMapFunction[String, String] {
        var jsonParser: JsonParser = _


        override def open(parameters: Configuration): Unit = {
          jsonParser = new JsonParser()
        }

        override def map(in: String): String = {

          val json = jsonParser.parse(in).getAsJsonObject

          val sqlOperator = json.get("operator_type").getAsString

          var data: JsonObject = null
          var result = ""


          if ("r".equals(sqlOperator) || "u".equals(sqlOperator) || "c".equals(sqlOperator)) {
            // read / create / u

            data = json.get("after").getAsJsonObject
            // add
            data.addProperty("__op", "0")

          } else if ("d".equals(sqlOperator)) {
            //
            data = json.get("before").getAsJsonObject
            data.addProperty("__op", "1")
          }

          if (data != null && !data.isJsonNull) {
            result = data.toString
          }

          result

        }

      })
      .name("map")
      .uid("map")

    map.addSink(sink).name("sink").uid("sink")

    env.execute("CdcAdsCircleGuestTaskResult")
  }

}
