package com.venn.stream.api.checkpoint

import java.time.Duration

import com.venn.entity.KafkaSimpleStringRecord
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.StateTtlConfig.{StateVisibility, UpdateType}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 测试 flink state 过期后，Checkpoint 恢复还有吗？
 * 1 分钟 Checkpoint，state ttl 5 分钟
 * 从 3分钟以后的 Checkpoint 恢复，状态还在吗 ，状态的有效期是多长呢？ yes, 2 minute
 * 从 5分钟以后的 Checkpoint 恢复，状态还在吗？ no
 * --- test result
 * Checkpoint 恢复后，状态的有效期是从第一次的时间开始算的
 * 从 5 分钟后的 Checkpoint 恢复，状态不在了
 * 从 5 分钟内的 Checkpoint 恢复，状态还在，恢复后的状态的有效期，是从第一次写入时间开始计算的
 * 停止任务超过5分钟，从第 1 分钟的 Checkpoint 恢复，状态不在了
 *
 * 写数据后等 Checkpoint 完成，使用该 Checkpoint 马上启动任务，状态数据还在
 *                          使用该 Checkpoint 等 5 分钟启动任务，状态数据不在了
 *
 * 结论： 使用 state ttl，Checkpoint 恢复后，ttl 时间是从第一次写入时间开始算的
 */
object StateTtlTest {

  def main(args: Array[String]): Unit = {

    // end
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // checkpoint
    // 每 1000ms 开始一次 checkpoint
    env.enableCheckpointing(60 * 1000)
    // 高级选项：
    // 设置模式为精确一次 (这是默认值)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // 确认 checkpoints 之间的时间会进行 500 ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // 允许两个连续的 checkpoint 错误
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)

    // 同一时间只允许一个 checkpoint 进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // storage path
    val checkpointStorage = new FileSystemCheckpointStorage("hdfs:///user/wuxu/checkpoint/state_ttl")
    env.getCheckpointConfig.setCheckpointStorage(checkpointStorage)
    // rocksdb
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))

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

    env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "source")
      .name("source")
      .uid("source")
      .keyBy(new KeySelector[KafkaSimpleStringRecord, String] {
        override def getKey(value: KafkaSimpleStringRecord): String = {
          // keyby all record to same key
          "1"
        }
      })
      .process(new KeyedProcessFunction[String, KafkaSimpleStringRecord, String] {

        var valueState: ValueState[String] = _

        override def open(parameters: Configuration): Unit = {

          val ttl = StateTtlConfig.newBuilder(Time.minutes(5))
            .setStateVisibility(StateVisibility.NeverReturnExpired)
            .setUpdateType(UpdateType.OnCreateAndWrite)
            .build()

          val stateDescriptor = new ValueStateDescriptor[String]("value", classOf[String])
          // ttl
          stateDescriptor.enableTimeToLive(ttl)
          valueState = getRuntimeContext.getState(stateDescriptor)

        }

        override def processElement(element: KafkaSimpleStringRecord, ctx: KeyedProcessFunction[String, KafkaSimpleStringRecord, String]#Context, out: Collector[String]): Unit = {
          val current = System.currentTimeMillis()
          val time = DateTimeUtil.formatMillis(current, DateTimeUtil.YYYY_MM_DD_HH_MM_SS)
          // check variable null
          //          if (valueState == null) {
          //            println(time + " - valueState is null")
          //            // set value
          //            valueState.update(element.getValue)
          //          } else {
          // check value is null
          if (valueState.value() == null) {
            println(time + " - valueState value is null")
            valueState.update(element.getValue)
          } else {
            // get value
            println(time + " - set valueState value is not null, value : " + valueState.value())
            valueState.update(element.getValue)
          }
          //          }


        }
      })

    env.execute("StateTtlTest")
  }
}
