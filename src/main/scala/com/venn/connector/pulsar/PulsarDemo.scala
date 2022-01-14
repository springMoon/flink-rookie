package com.venn.connector.pulsar

import java.time.Duration

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.pulsar.source.PulsarSource
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.flink.api.scala._
import com.venn.common.Common.PULSAR_SERVER
import com.venn.common.Common.PULSAR_ADMIN
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

object PulsarDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val pulsarSource = PulsarSource.builder()
      .setServiceUrl(PULSAR_SERVER)
      .setAdminUrl(PULSAR_ADMIN)
      .setStartCursor(StartCursor.earliest())
      .setTopics("user_log")
      .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
      .setSubscriptionName("my-subscription")
      .setSubscriptionType(SubscriptionType.Exclusive)
      .build()

    //env.fromSource(pulsarSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
    env.fromSource(pulsarSource, WatermarkStrategy.noWatermarks(), "pulsar")
      .map(str => str)
      .process(new ProcessFunction[String, String] {
        var count: Long = 0

        override def processElement(element: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
          count += 1
          if (count % 1000 == 0) {
            println("count: ", count)
          }
        }
      })


    env.execute("pulsar demo")


  }

}
