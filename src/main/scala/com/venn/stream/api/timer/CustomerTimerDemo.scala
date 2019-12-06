package com.venn.stream.api.timer

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.util
import java.util.{Timer, TimerTask}
import org.apache.flink.api.scala._
import com.venn.common.Common
import com.venn.util.TwoStringSource
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.LoggerFactory

/**
  * 在 open 方法中使用 定时器，定时加载外部数据，比如mysql
  * 业务假设： ETL的时候，数据进来，需要补充外部系统的数据，外部系统的数据会更新，所有不能一次性加载就不管了
  * 又有大部分数据是不会更新的（或者更新只是偶尔的），如果使用异步io 感觉很浪费
  * 这时候就可以考虑，使用timer，定时加载
  *
  *
  * 在map 中连接，添加定时器，定时从mysql 加载数据
  */
object CustomerTimerDemo {
  private final val logger = LoggerFactory.getLogger(CustomerTimerDemo.getClass)

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if ("/".equals(File.separator)) {
      val backend = new FsStateBackend(Common.CHECK_POINT_DATA_DIR, true)
      env.setStateBackend(backend)
      env.enableCheckpointing(30 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE)
    } else {
      env.setMaxParallelism(1)
      env.setParallelism(1)
    }

    // 自定义的source，输出 x,xxx 格式随机字符
    val input = env.addSource(new TwoStringSource)
    val stream = input.map(new RichMapFunction[String, String] {

      val jdbcUrl = "jdbc:mysql://venn:3306?useSSL=false&allowPublicKeyRetrieval=true"
      val username = "root"
      val password = "123456"
      val driverName = "com.mysql.jdbc.Driver"
      var conn: Connection = null
      var ps: PreparedStatement = null
      val map = new util.HashMap[String, String]()

      override def open(parameters: Configuration): Unit = {
        logger.info("init....")
        query()
        // new Timer
        val timer = new Timer(true)
        // schedule is 10 second, 1 second between successive task executions
        timer.schedule(new TimerTask {
          override def run(): Unit = {
            query()
          }
        }, 10000)

      }

      override def map(value: String): String = {
        // concat input and mysql data
        value + "-" + map.get(value.split(",")(0))
      }

      /**
        * query mysql for get new config data
        */
      def query() = {
        logger.info("query mysql")
        try {
          Class.forName(driverName)
          conn = DriverManager.getConnection(jdbcUrl, username, password)
          ps = conn.prepareStatement("select id,name from venn.timer")
          val rs = ps.executeQuery

          while (!rs.isClosed && rs.next) {
            val id = rs.getString(1)
            val name = rs.getString(2)
            map.put(id, name)
          }
          logger.info("get config from db size : {}", map.size())

        } catch {
          case e@(_: ClassNotFoundException | _: SQLException) =>
            e.printStackTrace()
        } finally {
          if (conn != null) {
            conn.close()
          }
        }
      }
    })
//              .print()


    val sink = new FlinkKafkaProducer[String]("timer_out"
      , new SimpleStringSchema()
      , Common.getProp)
    stream.addSink(sink)
    env.execute(this.getClass.getName)

  }

}
