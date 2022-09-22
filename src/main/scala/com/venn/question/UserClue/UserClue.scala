//package com.venn.question.UserClue
//
//import com.venn.entity.KafkaSimpleStringRecord
//import com.venn.util.SimpleKafkaRecordDeserializationSchema
//import org.apache.flink.api.common.eventtime.WatermarkStrategy
//import org.apache.flink.api.common.functions.RichMapFunction
//import org.apache.flink.connector.kafka.source.KafkaSource
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
//import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
//import org.apache.flink.formats.json.JsonNodeDeserializationSchema
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper
//import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
//import org.apache.flink.api.scala._
//
//import java.util
//
//object UserClue {
//  val bootstrapServer = "localhost:9092"
//  val topic = "user_log"
//  val sinkTopic = "user_log_sink"
//
//  def main(args: Array[String]): Unit = {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    val source = KafkaSource
//      .builder()[ObjectNode]
//      .setBootstrapServers(bootstrapServer)
//      .setGroupId("MyGroup")
//      .setClientIdPrefix("aa")
//      .setTopics(util.Arrays.asList("user_log"))
//            .setDeserializer(KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true)))
////      .setDeserializer(new KafkaDeserializationSchemaWrapper())
//      //      .setStartingOffsets(OffsetsInitializer.earliest())
//      .setStartingOffsets(OffsetsInitializer.latest())
//      .build()
//
//    env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
//      .map(new RichMapFunction[ObjectNode, ObjectNode] {
//        override def map(node: ObjectNode): ObjectNode = {
//
//         val userId = node.get("user_id")
//
//          node
//        }
//      })
//
//
//
//  }
//
//}
