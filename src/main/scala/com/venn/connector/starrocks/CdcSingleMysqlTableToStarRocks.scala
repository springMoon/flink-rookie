package com.venn.connector.starrocks

import com.google.gson.{JsonObject, JsonParser}
import com.starrocks.connector.flink.StarRocksSink
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions
import com.venn.connector.cdc.DdlDebeziumDeserializationSchema
import com.venn.util.DateTimeUtil
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset
import com.ververica.cdc.connectors.mysql.table.{StartupMode, StartupOptions}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory

import java.util.Properties

/**
 * @Classname CdcAdsCircleGuestTaskResult
 * @Description TODO
 * @Date 2024/3/6
 * @Created by venn
 */
object CdcSingleMysqlTableToStarRocks {

  val LOG = LoggerFactory.getLogger("CdcAdsCircleGuestTaskResult")
  //  val ip = "10.20.131.192"
  //  val jdbcPort = "9030"
  //  val httpPort = "18030"
  //  val user = "root"
  //  val pass = "showyu123"
  //  var batch = 64000
  //  var interval = 5
  //
  //  var sourceId = "rm-2ze82f881xft670dm.mysql.rds.aliyuncs.com"
  //  var sourceUser = "deepexi"
  //  var sourcePass = "mvqRxerKIgQadEO1M74"


  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      LOG.error("please input the cdc config file as parameter")
    }
    val file = args(0)
    val parameterTool = ParameterTool.fromPropertiesFile(file)


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.disableOperatorChaining()

    var startupOption = StartupOptions.latest()
    if (StartupMode.INITIAL.equals(parameterTool.get("source.startup_option"))) {
      startupOption = StartupOptions.initial()
    } else if (StartupMode.EARLIEST_OFFSET.equals(parameterTool.get("source.startup_option"))) {
      startupOption = StartupOptions.earliest()
    } else if (StartupMode.TIMESTAMP.equals(parameterTool.get("source.startup_option"))) {
      val startTime = DateTimeUtil.parse(parameterTool.get("source.startup_option_time"))
      startupOption = StartupOptions.timestamp(startTime.getTime * 1000)
    } else if (StartupMode.SPECIFIC_OFFSETS.equals(parameterTool.get("source.startup_option"))) {
      val offset = BinlogOffset.ofBinlogFilePosition(parameterTool.get("source.startup_option.offset.file"), parameterTool.get("source.startup_option.offset.position").toLong)
      startupOption = StartupOptions.specificOffset(offset)
    } else {
      LOG.warn("source.startup_option default is latest")
    }


    val prop = new Properties()
    prop.put("converters", "dateConverters")
    prop.put("dateConverters.type", "com.venn.common.MySqlDateTimeConverter")


    // cdc source
    val source = MySqlSource.builder[String]()
      .hostname(parameterTool.get("source.host"))
      .port(parameterTool.get("source.port").toInt)
      .username(parameterTool.get("source.user"))
      .password(parameterTool.get("source.pass"))
      .databaseList(parameterTool.get("source.database"))
      .tableList(parameterTool.get("source.table_list"))
      .serverTimeZone(parameterTool.get("source.time_zone"))
      // 包含 schema change
      .includeSchemaChanges(false)
      .debeziumProperties(prop)
      //      .startupOptions(StartupOptions.latest())
      .startupOptions(startupOption)
      .deserializer(new DdlDebeziumDeserializationSchema(parameterTool.get("source.host"), parameterTool.get("source.port").toInt))
      .build()

    // todo, set with start command is better
    env.setParallelism(1)


    val sink = StarRocksSink.sink(
      // the sink options
      StarRocksSinkOptions.builder()
        .withProperty("jdbc-url", parameterTool.get("sink.jdbc-url"))
        .withProperty("load-url", parameterTool.get("sink.load-url"))
        .withProperty("username", parameterTool.get("sink.username"))
        .withProperty("password", parameterTool.get("sink.password"))
        .withProperty("database-name", parameterTool.get("sink.database-name"))
        .withProperty("table-name", parameterTool.get("sink.table-name"))
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
        .withProperty("sink.buffer-flush.max-rows", parameterTool.get("sink.batch"))
        .withProperty("sink.buffer-flush.interval-ms", parameterTool.get("sink.interval"))
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

    env.execute(parameterTool.get("job_name"))
  }

}
