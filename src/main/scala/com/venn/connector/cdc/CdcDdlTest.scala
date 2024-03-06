package com.venn.connector.cdc

import com.venn.source.mysql.cdc.CommonStringDebeziumDeserializationSchema
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import java.util.TimeZone

/**
 * @Classname CdcDdlTest
 * @Description TODO
 * @Date 2023/8/31
 * @Created by venn
 */
object CdcDdlTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // cdc source
    val source = MySqlSource.builder[String]()
      .hostname("rm-2ze0qoq964s4nnodi.mysql.rds.aliyuncs.com")
      .port(3306)
      .username("daas")
      .password("Dass@2021")
      .databaseList("dct3_0")
      .tableList("dct3_0.*")
      .serverTimeZone("Asia/Shanghai")
      // 包含 schema change
      .includeSchemaChanges(true)
      .startupOptions(StartupOptions.latest())
      .deserializer(new DdlDebeziumDeserializationSchema("", 3306))
      .build()



    env.setParallelism(1)
    env.fromSource(source, WatermarkStrategy.noWatermarks[String](), "cdc")
      .map((str: String) => str)
      .print()


    env.execute("CdcDdlTest")

  }

}
