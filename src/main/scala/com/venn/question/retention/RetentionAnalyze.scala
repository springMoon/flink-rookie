package com.venn.question.retention

import java.time.Duration

import com.google.gson.JsonParser
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * user day retention analyze
 * 第1日留存率（即“次留”）：（当天新增的用户中，新增日之后的第1天还登录的用户数）/第一天新增总用户数；
 */
object RetentionAnalyze {

  val checkpointPath = "hdfs:///user/wuxu/checkpoint/RetentionAnalyze"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    // checkpoint
    setCheckPoint(env, checkpointPath)

    // source
    val source = KafkaSource
      .builder[KafkaSimpleStringRecord]()
      // stop job when consumer to latest offset ?
      //      .setBounded(OffsetsInitializer.latest())
      //      .setUnbounded(OffsetsInitializer.latest())
      .setBootstrapServers("localhost:9092")
      .setGroupId("ttl")
      .setTopics("user_log")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
      .name("source")
      .uid("source")
      .map(new RichMapFunction[KafkaSimpleStringRecord, UserLog] {

        var jsonParse: JsonParser = _

        override def open(parameters: Configuration): Unit = {
          jsonParse = new JsonParser
        }

        override def map(element: KafkaSimpleStringRecord): UserLog = {
          val jsonObject = jsonParse.parse(element.getValue).getAsJsonObject
          val userId = jsonObject.get("user_id").getAsString
          val categoryId = jsonObject.get("category_id").getAsInt
          val itemId = jsonObject.get("item_id").getAsInt
          val behavior = jsonObject.get("behavior").getAsString
          val ts = jsonObject.get("ts").getAsString
          val userLog = UserLog(userId, categoryId, itemId, behavior, ts)

          userLog
        }
      })
      .assignAscendingTimestamps(userLog => DateTimeUtil.parse(userLog.ts).getTime)
      // todo
      //      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
      .keyBy(new KeySelector[UserLog, String] {
        override def getKey(element: UserLog): String = {
          "1"
        }
      })


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
