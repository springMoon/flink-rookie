package com.venn.connector.starrocks

import com.starrocks.connector.flink.StarRocksSink
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

import scala.util.Random

object StreamLoadTestV2 {

  val LOG = LoggerFactory.getLogger("StreamLoadTest")
  val COL_SEP = "\\\\x01";
  val ROW_SEP = "\\\\x02";
  val ip = "10.201.0.230"
  val jdbcPort = "29030"
  val httpPort = "28030"
  val user = "root"
  val pass = "123456"
  val sql = "select * from test.t_starrocks_load_error limit 2000"
  var batch = 1000
  var interval = 5

  def main(args: Array[String]): Unit = {

    if (args.length >= 2) {
      batch = Integer.parseInt(args(0))
      interval = Integer.parseInt(args(1))
    }


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = env.addSource(new CustJdbcSource(ip, jdbcPort, user, pass, sql, COL_SEP, batch, interval))

    val stream = source.map(new RichMapFunction[String, String] {

      var random: Random = _

      override def open(parameters: Configuration): Unit = {
        random = new Random();

      }

      override def map(element: String): String = {

        val index = element.indexOf(COL_SEP)

        val prex = element.substring(0, index)
        val subx = element.substring(index)

        var newPrex = 0l
        try {
          newPrex = prex.toLong / (random.nextInt(10000) + 1)
        } catch {
          case ex: java.lang.ArithmeticException =>
            newPrex = random.nextLong()
            ex.printStackTrace()
            LOG.info("prex : {}", prex)

          case _ =>

        }

        newPrex + subx
      }
    })

    val sink = StarRocksSink.sink(
      // the sink options
      StarRocksSinkOptions.builder()
        .withProperty("jdbc-url", "jdbc:mysql://" + ip + ":" + jdbcPort)
        .withProperty("load-url", ip + ":" + httpPort)
        .withProperty("username", user)
        .withProperty("password", pass)
        .withProperty("database-name", "test")
        .withProperty("table-name", "t_starrocks_load_error_3")
        // 自 2.4 版本，支持更新主键模型中的部分列。您可以通过以下两个属性指定需要更新的列。
        // .withProperty("sink.properties.partial_update", "true")
        // .withProperty("sink.properties.columns", "k1,k2,k3")
        //        .withProperty("sink.properties.format", "json")
        //        .withProperty("sink.properties.strip_outer_array", "true")
        .withProperty("sink.properties.row_delimiter", ROW_SEP)
        .withProperty("sink.properties.column_separator", COL_SEP)
        // 设置并行度，多并行度情况下需要考虑如何保证数据有序性
        .withProperty("sink.parallelism", "1")
        .withProperty("sink.buffer-flush.max-rows", "" + batch)
        .build())

    stream.addSink(sink)
      .uid("sink")
      .name("sink")

    env.execute("StreamLoadTest")

  }
}
