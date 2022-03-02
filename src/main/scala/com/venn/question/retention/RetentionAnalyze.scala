package com.venn.question.retention


import com.google.gson.JsonParser
import com.venn.common.Common
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.sql.PreparedStatement
import java.util

/**
 * user day retention analyze
 * 第1日留存率（即“次留”）：（当天新增的用户中，新增日之后的第1天还登录的用户数）/第一天新增总用户数；
 */
object RetentionAnalyze {

  val LOG = LoggerFactory.getLogger("RetentionAnalyze")
  val bootstrapServer = "localhost:9092"
  val topic = "user_log"
  val sinkTopic = "user_log_sink"
  //  val checkpointPath = "hdfs:///user/wuxu/checkpoint/RetentionAnalyze"
  val checkpointPath = "file:///tmp/checkpoint/RetentionAnalyze"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // checkpoint
    setCheckPoint(env, checkpointPath)

    // source
    val kafkaSource = KafkaSource
      .builder[KafkaSimpleStringRecord]()
      // stop job when consumer to latest offset ?
      //      .setBounded(OffsetsInitializer.latest())
      //      .setUnbounded(OffsetsInitializer.latest())
      .setBootstrapServers(bootstrapServer)
      .setGroupId("ra")
      .setTopics(topic)
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .build()

    // 不使用 IngestionTime 指定 watermark，后续从数据中提取时间戳和 watermark
    val source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "source")
      .name("source")
      .uid("source")

    val stream = source.flatMap(new RichFlatMapFunction[KafkaSimpleStringRecord, UserLog] {
      var jsonParse: JsonParser = _

      override def open(parameters: Configuration): Unit = {
        jsonParse = new JsonParser
      }

      // parse json to UserLog
      override def flatMap(element: KafkaSimpleStringRecord, out: Collector[UserLog]): Unit = {
        try {
          val jsonObject = jsonParse.parse(element.getValue).getAsJsonObject
          val userId = jsonObject.get("user_id").getAsString
          val categoryId = jsonObject.get("category_id").getAsInt
          val itemId = jsonObject.get("item_id").getAsInt
          val behavior = jsonObject.get("behavior").getAsString
          val ts = jsonObject.get("ts").getAsString
          val tsLong = DateTimeUtil.parse(ts).getTime
          val userLog = UserLog(userId, categoryId, itemId, behavior, ts, tsLong)

          out.collect(userLog)
        } catch {
          case _ =>
            LOG.warn("parse json error : " + element.getValue)
        }

      }
    })
      .name("map")
      .uid("map")
      // deprecated assignTimestampsAndWatermarks
      //      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[UserLog] {
      //        var timestamp: Long = _
      //
      //        override def getCurrentWatermark: watermark.Watermark = {
      //
      //          println(timestamp)
      //          new watermark.Watermark(timestamp)
      //        }
      //
      //        override def extractTimestamp(element: UserLog, recordTimestamp: Long): Long = {
      //          timestamp = DateTimeUtil.parse(element.ts).getTime
      //          timestamp
      //        }
      //      })
      // default is IngestionTime, kafka source will add timestamp to StreamRecord,
      // if not set assignAscendingTimestamps, use StreamRecord' timestamp, so is ingestion time
      .assignAscendingTimestamps(userLog => userLog.tsLong)
      // create watermark by all elements
      .assignTimestampsAndWatermarks(new WatermarkStrategy[UserLog] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[UserLog] = {
          new WatermarkGenerator[UserLog] {
            var watermark: Watermark = new Watermark(Long.MinValue)

            override def onEvent(element: UserLog, eventTimestamp: Long, output: WatermarkOutput): Unit = {
              watermark = new Watermark(element.tsLong - 1)
              output.emitWatermark(watermark)
            }

            override def onPeriodicEmit(output: WatermarkOutput): Unit = {
              output.emitWatermark(watermark)
            }
          }
        }
      })
      // key all data to one key
      .keyBy(new KeySelector[UserLog, String] {
        override def getKey(element: UserLog): String = {
          "1"
        }
      })
      .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
      //      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10 * 60)))
      //      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      // 做天的窗口，初始化的时候加载昨日新增用户列表
      // 按天划分，触发器触发计算
      // 窗口结束的将新用户替换昨日新增用户列表
      // 实时计算当日的用户留存率： 第1日留存率（即“次留”）：（当天新增的用户中，新增日之后的第1天还登录的用户数）/第一天新增总用户数；
      .process(new RetentionAnalyzeProcessFunction)
      .name("process")
      .uid("process")

    // sink current day user to mysql, cost a lot time
    val sideTag = new OutputTag[(String, util.HashMap[String, Int])]("side")
    val jdbcSink = JdbcSink
      .sink("insert into user_info(user_id, login_day) values(?, ?)", new JdbcStatementBuilder[(String, String)] {
        override def accept(ps: PreparedStatement, element: (String, String)): Unit = {
          ps.setString(1, element._2)
          ps.setString(2, element._1)
        }
      }, JdbcExecutionOptions.builder()
        .withBatchSize(100)
        .withBatchIntervalMs(200)
        .withMaxRetries(5)
        .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:mysql://localhost:3306/venn?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true")
          .withDriverName("com.mysql.cj.jdbc.Driver")
          .withUsername("root")
          .withPassword("123456")
          .build())

    stream.getSideOutput(sideTag)
      .flatMap(new RichFlatMapFunction[(String, util.HashMap[String, Int]), (String, String)]() {
        override def flatMap(element: (String, util.HashMap[String, Int]), out: Collector[(String, String)]): Unit = {
          val day = element._1
          val keySet = element._2.keySet()
          keySet.forEach(item => {
            out.collect((day, item))
          })
        }
      })
      .addSink(jdbcSink)
      .name("jdbcSink")
      .uid("jdbcSink")


    val kafkaSink = KafkaSink
      .builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setKafkaProducerConfig(Common.getProp)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(sinkTopic)
        .setKeySerializationSchema(new SimpleStringSchema())
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()

    stream.sinkTo(kafkaSink)
      .name("sinkKafka")
      .uid("sinkKafka")

    env.execute("RetentionAnalyze")
  }

  // set checkpoint
  private def setCheckPoint(env: StreamExecutionEnvironment, checkpointPath: String) = {
    // 每 1000ms 开始一次 checkpoint
    env.enableCheckpointing(5 * 60 * 1000)
    // 高级选项：
    // 设置模式为精确一次 (这是默认值)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // 确认 checkpoints 之间的时间会进行 500 ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5 * 60 * 1000)

    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
    env.getCheckpointConfig.setCheckpointTimeout(10 * 60 * 1000)

    // 允许两个连续的 checkpoint 错误
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(10)

    // 同一时间只允许一个 checkpoint 进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // storage path
    val checkpointStorage = new FileSystemCheckpointStorage(checkpointPath)
    env.getCheckpointConfig.setCheckpointStorage(checkpointStorage)
    // rocksdb
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
  }
}
