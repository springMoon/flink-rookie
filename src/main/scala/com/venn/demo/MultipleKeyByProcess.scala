package com.venn.demo

import com.venn.common.Common
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStreamUtils
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  * try replace keyBy.process.keyBy.process.keyBy.process to DataStreamUtils#reinterpretAsKeyedStream
  *
  * 必须要先 keyby 一次（或者说，stream 数据必须是分区的） (不同分区的数据进来，使用状态的时候，会报  NullPointException)
  */
object MultipleKeyByProcess {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val topic = "randon_string"
    val kafkaSource = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), Common.getProp)

    val source: DataStream[(String, String, String)] = env.addSource(kafkaSource)
      .map(str => {
        val arr = str.split(",")
        (arr(0), arr(1), arr(2))
      })

    ////// keyby
    /*source
      .keyBy(0)
      .process(new TmpKeyedProcessFunction2)
      .keyBy(0)
      .process(new TmpKeyedProcessFunction2)
      .keyBy(0)
      .process(new TmpKeyedProcessFunction2)*/

    //////////////////////////////////////////////////
    val keyStream0 = source.keyBy(0)
      .process(new TmpKeyedProcessFunction2)

    val keyedStream = new DataStreamUtils(keyStream0)
      .reinterpretAsKeyedStream(element => element._1)
      .process(new TmpKeyedProcessFunction("11"))

    val keyedStream2 = new DataStreamUtils(keyedStream)
      .reinterpretAsKeyedStream(element => element._1)
      .process(new TmpKeyedProcessFunction("22"))

    env.execute("multiKeyBy")
  }

  /**
    * 如果前面没有keyby，valueState 不可以，有时会报  NPE
    *
    * @param flag
    */
  class TmpKeyedProcessFunction(flag: String) extends KeyedProcessFunction[String, (String, String, String), (String, String, String)] {

    var valueForKey: ValueState[String] = _

    override def open(parameters: Configuration): Unit = {
      valueForKey = getRuntimeContext.getState(new ValueStateDescriptor[String]("key", classOf[String]))
    }

    override def processElement(value: (String, String, String),
                                ctx: KeyedProcessFunction[String, (String, String, String), (String, String, String)]#Context,
                                out: Collector[(String, String, String)]): Unit = {

      val currentKey = value._1
      try {
        if (valueForKey.value() == null) {
          valueForKey.update(currentKey)
        } else if (currentKey.equals(valueForKey.value())) {
          // do nothing
        } else {
          println(flag + " current key : " + currentKey + ", last key : " + valueForKey.value())
          valueForKey.update(currentKey)
        }

      } catch {
        case e: Exception => {
          println(flag + " current key : " + currentKey + ", last key : " + valueForKey.value())
          e.printStackTrace()
        }
      }
      out.collect(value)
    }
  }

  class TmpKeyedProcessFunction3(flag: String) extends KeyedProcessFunction[String, (String, String, String), (String, String, String)] {

    var mapState: MapState[String, Byte] = _

    override def open(parameters: Configuration): Unit = {
      mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Byte]("map", classOf[String], classOf[Byte]))
    }

    override def processElement(value: (String, String, String), ctx: KeyedProcessFunction[String, (String, String, String), (String, String, String)]#Context, out: Collector[(String, String, String)]): Unit = {

      val currentKey = ctx.getCurrentKey
      if (!mapState.contains(currentKey)) {
        mapState.put(currentKey, 1)
      }
      var counter = 0
      val it = mapState.keys().iterator()
      while (it.hasNext) {
        val ke = it.next()
        counter += 1
      }
      if (counter > 1) {
        println("keys counter : " + counter)
      }
      out.collect(value)
    }
  }

  class TmpKeyedProcessFunction2 extends KeyedProcessFunction[Tuple, (String, String, String), (String, String, String)] {

    var valueForKey: ValueState[String] = _

    override def open(parameters: Configuration): Unit = {
      valueForKey = getRuntimeContext.getState(new ValueStateDescriptor[String]("key", classOf[String]))
    }

    override def processElement(value: (String, String, String), ctx: KeyedProcessFunction[Tuple, (String, String, String), (String, String, String)]#Context, out: Collector[(String, String, String)]): Unit = {

      val currentKey = value._1

      if (valueForKey.value() == null) {
        valueForKey.update(currentKey)

      } else if (currentKey.equals(valueForKey.value())) {

      } else {
        println("2222 current key : " + currentKey + ", last key : " + valueForKey.value())
        valueForKey.update(currentKey)
      }


      out.collect(value)
    }
  }

}
