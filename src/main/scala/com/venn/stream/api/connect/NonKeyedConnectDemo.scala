package com.venn.stream.api.connect

import java.util

import com.venn.common.Common
import com.venn.util.StringUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * non keyed connect demo
 * 问题：
 * 1、两个 non keyed 流 connect 的时候，数据是怎么分配的（并发：1,2,3）（并发不同的数据，数据怎么分，随机分配吗？太傻了吧）
 * 2、keyed 流 connect non keyed 流 的时候，数据是怎么分配的
 * 3、non keyed 流 connect keyed 流 的时候，数据是怎么分配的
 * 4、两个 keyed 流 connect 的时候，数据是怎么分配的
 * 两个流的 keyBy 都是对 CoProcessFunction 的并发做的分区，所以相同 key 的数据一定会发到一起
 */
object NonKeyConnectDemo {

  val logger = LoggerFactory.getLogger(NonKeyConnectDemo.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.setAutoWatermarkInterval(0)

    env.setParallelism(4)
    // 配置更新流
    val config = new FlinkKafkaConsumer[String]("dynamic_config", new SimpleStringSchema, Common.getProp)

    val configStream = env
      .addSource(config)
      .setParallelism(1)
      .name("configStream")
    //      .broadcast

    val input = env.addSource(new RadomSourceFunction)
      .name("radomFunction")
      .map(str => str)
      .setParallelism(4)


    val stream = input.connect(configStream)
      .process(new CoProcessFunction[String, String, String] {
        //        var mapState: MapState[String, String] = _
        var map: util.HashMap[String, String] = null

        override def open(parameters: Configuration): Unit = {
          // thinking broken, if use keyed state, must keyby upstream
          // Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.
          //mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, String]("mapState", classOf[String], classOf[String]))
          map = new util.HashMap[String, String]()
        }

        override def processElement1(element: String, context: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
          // checkouk map keys
          val size = map.size()
          //          println("keys size : " + size)

          val citeInfo = element.split(",")
          val code = citeInfo(0)
          //          var va = mapState.get(code)
          var va = map.get(code)
          // 不能转码的数据默认输出 中国(code=xxx)
          if (va == null) {
            va = "中国(code=" + code + ")";
          } else {
            va = va + "(code=" + code + ")"
          }
          //          println(getRuntimeContext.getIndexOfThisSubtask + ", " + va)
          out.collect(va + "," + citeInfo(1))
        }

        override def processElement2(element: String, context: CoProcessFunction[String, String, String]#Context, collector: Collector[String]): Unit = {

          println(getRuntimeContext.getIndexOfThisSubtask + ", " + element)
          val param = element.split(",")
          // update mapState
          //          mapState.put(param(0), param(1))
          map.put(param(0), param(1))
        }

        override def close(): Unit = {
          //          mapState.clear()
        }
      }).setParallelism(4)
    val sink = new FlinkKafkaProducer[String]("non_key_connect_demo", new SimpleStringSchema(), Common.getProp)
    //    stream.print()

    //      .addSink(sink)

    env.execute("NonKeyedConnectDemo")
  }
}

/*
class RadomSourceFunction extends SourceFunction[String] {
  var flag = true

  override def cancel(): Unit = {
    flag = false
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (flag) {
      for (i <- 0 to 300) {
        var nu = i.toString
        while (nu.length < 3) {
          nu = "0" + nu
        }
        ctx.collect(nu + "," + StringUtil.getRandomString(5))
        Thread.sleep(2000)
      }
    }
  }
}*/
