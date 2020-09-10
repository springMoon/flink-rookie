package com.venn.index.question.dynamicWindow

import java.util

import com.google.gson.Gson
import com.venn.common.Common
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * 动态窗口 demo
  */
object DyWindowDemo {

  private val logger = LoggerFactory.getLogger(DyWindowDemo.getClass)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5 * 60 * 1000)
    val stateBackend: StateBackend = new FsStateBackend(Common.CHECK_POINT_DATA_DIR)
    env.setStateBackend(stateBackend)
    env.setParallelism(1)

    val commandState = new MapStateDescriptor[String, Command]("commandState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[Command]() {}))
    val commandTopic = "command_topic"
    val commandSource = new FlinkKafkaConsumer[String](commandTopic, new SimpleStringSchema(), Common.getProp)
    val commandStream = env.addSource(commandSource)
      .flatMap(new RichFlatMapFunction[String, Command] {
        var gson: Gson = _
        override def open(parameters: Configuration): Unit = {
          gson = new Gson()
        }

        override def flatMap(element: String, out: Collector[Command]): Unit = {
          try {
            val command = gson.fromJson(element, classOf[Command])

            if (command != null) {
              out.collect(command)
            }
          } catch {
            case e: Exception =>
              logger.warn("parse command error : " + element, e)
          }

        }
      })
      .broadcast(commandState)

    val dataStream = env.addSource(new DataSourceFunction)
      .flatMap(new RichFlatMapFunction[String, DataEntity] {
        var gson: Gson = _

        override def open(parameters: Configuration): Unit = {
          gson = new Gson()
        }

        override def flatMap(element: String, out: Collector[DataEntity]): Unit = {
          try {
            val data = gson.fromJson(element, classOf[DataEntity])
            if (data != null) {
              out.collect(data)
            }
          } catch {
            case e: Exception =>
              logger.warn("parse input data error: {}" + element, e)
          }
        }
      })

    // connect stream
    val connectStream = dataStream
      .connect(commandStream)
      .process(new BroadcastProcessFunction[DataEntity, Command, (DataEntity, Command)]() {

        // 存放当前命令的 map
        var currentCommand: util.HashMap[String, Command] = _
        // 存放新命令的 map
        var commandState: MapStateDescriptor[String, Command] = _

        override def open(parameters: Configuration): Unit = {

          currentCommand = new util.HashMap[String, Command]()
          commandState = new MapStateDescriptor[String, Command]("commandState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[Command]() {}))
        }

        override def processElement(element: DataEntity, ctx: BroadcastProcessFunction[DataEntity, Command, (DataEntity, Command)]#ReadOnlyContext, out: Collector[(DataEntity, Command)]): Unit = {
          // 命令可以是大于/小于当前时间
          // 小于当前时间的，直接添加即可,之前命令的窗口不会收到新数据，新数据直接进新命令的窗口
          // 大于当前时间的命令，不能直接与流一起往下游输出，等时间小于当前的 processTime 时间后，才会开始新窗口
          val command = ctx.getBroadcastState(commandState).get(element.attr)
          val current = currentCommand.get(element.attr)
          if (command != null && command.startTime <= ctx.currentProcessingTime()) {
            // 当新命令的时间小于当前的处理时间，替换旧命令
            currentCommand.put(element.attr, command)
          }
          // 如果当前命令为空，数据就不往下发送了
          if (current != null) {
            out.collect((element, current))
          }
          // command not exists, ignore it
        }

        override def processBroadcastElement(element: Command, ctx: BroadcastProcessFunction[DataEntity, Command, (DataEntity, Command)]#Context, out: Collector[(DataEntity, Command)]): Unit = {
          // only one command are new accepted, cover old command
          logger.info("receive command : " + element)
          ctx.getBroadcastState(commandState).put(element.targetAttr, element)
        }
      })
      .assignAscendingTimestamps(_._1.time)

    // todo process sum
    val sumStream = connectStream
      .keyBy(_._1.attr)
      .window(DynamicTumblingEventTimeWindows.of())
      .process(new DyProcessWindowFunction())
      .print("result:")

    env.execute("DyWindowDemo")
  }

}

case class Command(taskId: String, targetAttr: String, method: String, periodUnit: String, periodLength: Long, startTime: Long)

case class DataEntity(attr: String, value: Int, time: Long)

