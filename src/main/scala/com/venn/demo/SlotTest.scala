package com.venn.demo

import com.google.gson.JsonParser
import com.venn.common.Common
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.question.retention.RetentionAnalyze.{bootstrapServer, sinkTopic}
import com.venn.question.retention.UserLog
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.Properties

/**
 * test slot: diff slot share group operator cannot share group
 *
 * 不同的 slot share group 里面的 算子，不能共享 slot
 * slot share group 生效范围： 当前算子还后续 算子
 * like :   source -> map -> sink
 *          map.slotSharingGroup(aa)
 *          default slot sharing group:  source
 *          aa slot sharing group: map -> sink
 *
 * 默认所有算子在 'default' slot sharing group，即 设置 slotSharingGroup('default'), 也在 default 里面
 *
 *
 * 测试 同一个程序跑，同时跑多个，遇到的 kafka 事务 id 过期的问题
 *
 */
object SlotTest {

  val LOG = LoggerFactory.getLogger("BothProcessAndEventTime")
  val bootstrapServer = "localhost:9092"
  val topic = "user_log"
  var sinkTopic = "user_log_sink"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    if(args.length == 1){
      sinkTopic = args(0)
    }

    // source
    val kafkaSource = KafkaSource
      .builder[KafkaSimpleStringRecord]()
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
//            LOG.warn("parse json error : " + element.getValue)
            out.collect(new UserLog("aaaaaa", 1, 1, "aa", "2022-04-28 12:12:12:333", 0))
        }

      }
    })
      .name("map1")
      .uid("map1")
      .map(ff => ff.userId)
      .name("map2")
      .uid("map2")



    val prop = Common.getProp
    prop.setProperty("transaction.timeout.ms", "600000")

    val sink = KafkaSink
      .builder[String]()
      .setBootstrapServers(bootstrapServer)
      .setKafkaProducerConfig(Common.getProp)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(sinkTopic)
        .setKeySerializationSchema(new SimpleStringSchema())
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
//      .setTransactionalIdPrefix("xxx")
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .build()

    stream.sinkTo(sink)
      .name("sink")
      .uid("sink")

    env.execute("slotTest")
  }

}
