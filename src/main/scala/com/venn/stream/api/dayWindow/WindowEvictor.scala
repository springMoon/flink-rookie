package com.venn.stream.api.dayWindow

import java.io.File
import java.text.SimpleDateFormat

import com.venn.common.Common
import com.venn.source.TumblingEventTimeWindows
import com.venn.util.CheckpointUtil
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector

/**
 * test day index: use ContinuousProcessingTimeTrigger trigger calculation,
 * use Evictor evitor window element, reduce calculation
 *
 */
object WindowEvictor {

  def main(args: Array[String]): Unit = {
    // environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //    if ("\\".equals(File.pathSeparator)) {
    //      val rock = new RocksDBStateBackend(Common.CHECK_POINT_DATA_DIR)
    //      env.setStateBackend(rock)
    //       checkpoint interval
    //      env.enableCheckpointing(10000)
    //    }
    CheckpointUtil.setCheckpoint(env, "rocksdb", Common.CHECK_POINT_DATA_DIR, 10)

    val topic = "current_day"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val source = new FlinkKafkaConsumer[ObjectNode](topic, new JsonNodeDeserializationSchema(), Common.getProp)
    source.setStartFromLatest()
    val sink = new FlinkKafkaProducer[String](topic + "_out", new SimpleStringSchema(), Common.getProp)
    sink.setWriteTimestampToKafka(true)

    val stream = env.addSource(source)
      .map(node => {
        Eventx(node.get("id").asText(), node.get("createTime").asText(), node.get("amt").asText("0.0"))
      })
      .assignAscendingTimestamps(event => sdf.parse(event.createTime).getTime)
      .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
      .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
    // evictor after process, not need, use the elements.drop(1)
    // if all the element in window is evictor, the window function will not trigger calc
    // event use the processingTimeTrigger
    //      .evictor(TimeEvictor.of(Time.seconds(0), true))


    /*stream.reduce(new ReduceFunction[Eventx] {
      override def reduce(value1: Eventx, value2: Eventx): Eventx = {

      }
    })*/


    val pStream1 = stream.process(new ProcessAllWindowFunction[Eventx, String, TimeWindow] {

      // store the current interval count value, and sum value
      var countState: ValueState[Long] = _
      var sumState: ValueState[BigDecimal] = _

      override def open(parameters: Configuration): Unit = {
        // init countState and sumState, set sumState = BigDecimal("0.0")
        countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
        sumState = getRuntimeContext.getState(new ValueStateDescriptor[BigDecimal]("sumState", classOf[BigDecimal]))
        // No key set. This method should not be called outside of a keyed context.
        //          sumState.update(BigDecimal("0.0"))
      }

      override def process(context: Context, elements: Iterable[Eventx], out: Collector[String]): Unit = {
        // set default value
        if (sumState.value() == null) {
          sumState.update(BigDecimal("1.0"))
        }
        // update count
        var count = countState.value()
        count += elements.count(_ => true)
        countState.update(count)
        // update sum
        var sum = sumState.value()
        val it = elements.toIterator
        while (it.hasNext) {
          val currentElement = it.next()
          sum = sum.+(BigDecimal(currentElement.amt))
        }
        sumState.update(sum)

        out.collect("sum=" + sum.toString)
        out.collect("count=" + count.toString)
      }
    })


    pStream1.print()

    // execute job
    env.execute("WindowEvictor")
  }

}

case class Eventx(id: String, createTime: String, amt: String = "0.0")

