package com.venn.demo

import com.venn.common.Common
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.scala._

object SlotPartitionDemo {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val topic = "slot_partition"
    val source = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), Common.getProp)
    val sink = new FlinkKafkaProducer[String](topic+"_out", new SimpleStringSchema(), Common.getProp)

    env.setParallelism(2)
    env.addSource(source)
      .addSink(sink)


    env.execute(this.getClass.getName)
  }

}
