package com.venn.question.tryFlink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.util.Random

/**
 * source from : flink official website : 基于 DataStream API 实现欺诈检测
 */
object FraudDetection {

  private val LOG = LoggerFactory.getLogger("FraudDetection")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = env.addSource(new FuaudDetectionSource)
      .name("source")

    val process = source
      .keyBy(_._1)
      .process(new FuaudDetectionProcessFunction)

    process.addSink(new SinkFunction[String]{
      override def invoke(element: String, context: SinkFunction.Context): Unit = {
        println("fraud detection alter : " + element)
      }
    } )

    env.execute("FuaudDetection")

  }

}

class FuaudDetectionSource extends SourceFunction[(String, Double)] {
  val LOG = LoggerFactory.getLogger("FuaudDetectionSource")
  var isRunning = true;
  val random = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[(String, Double)]): Unit = {

    while (isRunning) {
      val accountId = "" + random.nextInt(1000)
      val amt = random.nextDouble() * 100;

      sourceContext.collect(accountId, amt)

      Thread.sleep(1)
    }
    LOG.info("source finish")
  }

  override def cancel(): Unit = {

    LOG.info("source canceled...")
    isRunning = false;
  }
}

class FuaudDetectionProcessFunction extends KeyedProcessFunction[String, (String, Double), String] {

  var smallFlag: ValueState[java.lang.Boolean] = _

  override def open(parameters: Configuration): Unit = {
    smallFlag = getRuntimeContext.getState(new ValueStateDescriptor("smallTransaction", Types.BOOLEAN))
  }

  override def processElement(element: (String, Double), context: KeyedProcessFunction[String, (String, Double), String]#Context, collector: Collector[String]): Unit = {

    if(smallFlag.value() != null && smallFlag.value() && element._2 > 95){
      collector.collect(element._1)
    }

    if(element._2 < 2){
      smallFlag.update(true)
      context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 10 * 1000 )
    }

  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Double), String]#OnTimerContext, out: Collector[String]): Unit = {
    println("cliear key : " + ctx.getCurrentKey)
    smallFlag.clear()
  }

  override def close(): Unit = {
    smallFlag.clear()
  }
}
