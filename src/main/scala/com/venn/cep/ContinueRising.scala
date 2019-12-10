package com.venn.cep

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * Cep for price continue rising
  * CEP : 匹配价格连续上涨（keyby 可以匹配同一个商品价格连续上涨）
  *
  */
object ContinueRising {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 输入 id, volumn, name 三个字段的数据
    val input = env.addSource(new CepDemoSourceFunction)
      .map(str => {
        //        logger.info(str)
        val arr = str.split(",")
        val id = arr(0)
        val volume = arr(1).toInt
        val name = arr(2)
        CepDemoEvent(id, volume, name, arr(3).toInt)
      })
    //  Applying your pattern on a non-keyed stream will result in a job with parallelism equal to 1
    //      .keyBy(_.id)

    /**
      * 模式说明：
      * 匹配价格连续上涨
      *
      * 匹配后跳过策略： 默认从上次的开始事件后的下一个事件开始
      *
      */
    val pattern = Pattern.begin[CepDemoEvent]("first")
      .next("second").where(new IterativeCondition[CepDemoEvent] {
      override def filter(currentEvent: CepDemoEvent, context: IterativeCondition.Context[CepDemoEvent]): Boolean = {
        // get last event
        val firstList = context.getEventsForPattern("first").iterator()
        var lastStart: CepDemoEvent = null
        // get last from firstList, and get the last one
        while (firstList.hasNext) {
          lastStart = firstList.next()
        }
        if (currentEvent.volume > lastStart.volume) {
          true
        } else {
          false
        }
      }
    })
      // always remember add within, it will reduce the state usage
      .within(Time.minutes(5 * 60 * 1000))

    val patternStream = CEP.pattern(input, pattern)

    val result: DataStream[String] = patternStream.process(
      new PatternProcessFunction[CepDemoEvent, String]() {
        override def processMatch(
                                   events: util.Map[String, util.List[CepDemoEvent]],
                                   ctx: PatternProcessFunction.Context,
                                   out: Collector[String]): Unit = {
          // get the change
          val first = events.get("first").get(0)
          val second = events.get("second").get(0)
          val change = second.volume - first.volume
          out.collect("from : " + first.id + ", to " + second.id + ", change : " + change)
        }

      })

    // for convenient, just print
    result.print()
    env.execute(this.getClass.getName)
  }


}

