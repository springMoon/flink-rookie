package com.venn.cep

import java.util

import com.venn.util.MathUtil
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * 官网第一个案例实现
  *
  */
object CepDemo {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 输入 id, volumn, name 三个字段的数据
    val input = env.addSource(new CepDemoSourceFunction)
      .map(str => {
        logger.info(str)
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
      * 1、start : 匹配 id 等于 42 的模式
      * 2、middle : start 紧跟着 middle  volume 的值 大于 10.0,
      * subtype 该模式的多个条件，必须都满足？
      * 3、end ： middle 后面宽松的跟着 end， name 等于 end (不是紧跟着，中间可以插其他的数据)
      */
    val pattern = Pattern.begin[CepDemoEvent]("start").where(_.id.equals("42"))
      .next("middle").subtype(classOf[CepDemoEvent]).where(_.volume > 5)
      .subtype(classOf[CepDemoEvent]).where(_.name.equals("xx"))
      //      .next("middle").where(_.volume > 5.1)
      .next("test").where(new IterativeCondition[CepDemoEvent] {
      override def filter(currentEvent: CepDemoEvent, context: IterativeCondition.Context[CepDemoEvent]): Boolean = {
        // get last event
        val lastEventList = context.getEventsForPattern("start").iterator()
        var lastStart: CepDemoEvent = null
        if (lastEventList.hasNext) {
          lastStart = lastEventList.next()
        }
        if (currentEvent.volume > lastStart.volume) {
          true
        } else {
          false
        }
      }
    })
      .followedBy("end").where(_.name.equals("end"))

    val patternStream = CEP.pattern(input, pattern)

    val result: DataStream[String] = patternStream.process(
      new PatternProcessFunction[CepDemoEvent, String]() {
        override def processMatch(
                                   events: util.Map[String, util.List[CepDemoEvent]],
                                   ctx: PatternProcessFunction.Context,
                                   out: Collector[String]): Unit = {

          val start = events.get("start")
          val middle = events.get("middle")
          val end = events.get("end")
          // list 是因为规则后面可以加次数
          out.collect("start : " + start.get(0))
          out.collect("middle : " + middle.get(0))
          out.collect("end : " + end.get(0))

        }
      })

    // for convenient, just print
    result.print()
    env.execute(this.getClass.getName)
  }


}

case class CepDemoEvent(id: String, volume: Int, name: String, num: Int)

class CepDemoSourceFunction extends SourceFunction[String] {
  val logger = LoggerFactory.getLogger(this.getClass)

  var flag = true

  override def cancel(): Unit = {
    flag = false
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    var num = 1
    var id = 1
    while (flag) {

      val volumn = MathUtil.random.nextInt(10)
      val name = if (MathUtil.random.nextBoolean()) "xx" else "end"

      val message = id + "," + volumn + "," + name + "," + num
      //      logger.info(message)
      ctx.collect(message)

      id += 1
      if (id > 100) {
        id = 1
      }

      num += 1
      Thread.sleep(1000)

    }
    logger.info("{} cancel", this.getClass.getName)

  }
}


