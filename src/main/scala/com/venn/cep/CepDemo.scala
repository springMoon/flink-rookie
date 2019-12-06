package com.venn.cep

import java.util
import com.venn.util.MathUtil
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
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
        val arr = str.split(",")
        val id = arr(0)
        val volume = arr(1).toDouble
        val name = arr(2)
        CepDemoEvent(id, volume, name)
      })
      .keyBy(_.id)

    /**
      * 模式说明：
      * 1、start : 匹配 id 等于 42 的模式
      * 2、middle : start 紧跟着 middle  volume 的值 大于 10.0
      * 3、end ： middle 后面宽松的跟着 end， name 等于 end
      */
    val pattern = Pattern.begin[CepDemoEvent]("start").where(_.id.equals("42"))
      .next("middle").subtype(classOf[CepDemoEvent]).where(_.volume > 5.1)
      .followedBy("end").where(_.name == "end")

    val patternStream = CEP.pattern(input, pattern)

    val result: DataStream[CepDemoEvent] = patternStream.process(
      new PatternProcessFunction[CepDemoEvent, CepDemoEvent]() {
        override def processMatch(
                                   events: util.Map[String, util.List[CepDemoEvent]],
                                   ctx: PatternProcessFunction.Context,
                                   out: Collector[CepDemoEvent]): Unit = {

          val start = events.get("start")
          val middle = events.get("middle")
          val end = events.get("end")
          // list 是因为规则后面可以加次数
          out.collect(start.get(0))
          out.collect(middle.get(0))
          out.collect(end.get(0))

        }
      })

    // for convenient, just print
    result.print()
    env.execute(this.getClass.getName)
  }


}

case class CepDemoEvent(id: String, volume: Double, name: String)

class CepDemoSourceFunction extends SourceFunction[String] {
  val logger = LoggerFactory.getLogger(this.getClass)

  var flag = true

  override def cancel(): Unit = {
    flag = false
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    var id = 1
    while (flag) {

      val volumn = MathUtil.random.nextDouble() * 10
      val name = if (MathUtil.random.nextBoolean()) "xx" else "end"

      ctx.collect(id + "," + volumn + "," + name)

      id += 1
      if (id > 100) {
        id = 1
      }

    }
    logger.info("{} cancel", this.getClass.getName)

  }
}


