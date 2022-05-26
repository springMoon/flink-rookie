package com.venn.demo


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val configuration = new Configuration()
    configuration.setInteger(RestOptions.PORT, 8081)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    //设置并行度
    env.setParallelism(1)

    //设置事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //读取文件转成成样例类，提取时间戳指定事件时间和waterMark
//    val inputPath = "~/Downloads/UserBehavior.csv"
//    val inputSteam = env.readTextFile(inputPath)

    val properties = new Properties()
    // 从kafka读取数据
    properties.setProperty("bootstrap.servers", "192.168.10.101:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaDataStream = env.addSource(new FlinkKafkaConsumer[String]("hostItems", new SimpleStringSchema(), properties))
    val inputSteam = env.addSource(new FlinkKafkaConsumer[String]("hostItems", new SimpleStringSchema(), properties))

    val dataStream = inputSteam.map(data => {
      val lineArray = data.split(",")
      UserBehavior(lineArray(0).toLong, lineArray(1).toLong, lineArray(2).toInt, lineArray(3), lineArray(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val aggStream = dataStream.filter(_.behavior == "pv") //过滤PV行为
      .keyBy("itemId") //按照商品id分组
      .timeWindow(Time.hours(1), Time.minutes(5)) //滑动时间1小时，间隔5分钟
      .aggregate(new CountAgg(), new ItemViewResultWindow())

    // 按照窗口分组，收集当前窗口内的商品count数据
    val resultStream = aggStream.keyBy("windowEnd").process(new TopNHotItems(5))

    //    dataStream.print("data")
    //    aggStream.print("agg")
    resultStream.print()

    env.execute("hotitems_anlaysis")
  }

}

//自定义增量聚合函数AggregateFunction
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数windFunction
class ItemViewResultWindow() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {

  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windEnd, count))
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  // 先定义状态：ListState
  private var itemViewCountListState: ListState[ItemViewCount] = _

  //从运行环境获取状态值
  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据直接加入listState
    itemViewCountListState.add(value)
    // 注册一个windowEnd + 1之后触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 当定时器触发，可以认为所有窗口统计结果都已到齐，可以排序输出了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 为了方便排序，另外定义一个ListBuffer，保存ListState里面的所有数据
    val allItemViewCounts = new ListBuffer[ItemViewCount]
    val iter = itemViewCountListState.get().iterator()

    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }

    // 清空状态
    itemViewCountListState.clear()

    // 按照count大小排序，取前n个
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排名信息格式化成String，便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }

    result.append("\n==================================\n\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())

  }

}

// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
