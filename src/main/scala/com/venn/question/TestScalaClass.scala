package com.venn.question

import com.venn.entity.KafkaSimpleStringRecord
import com.venn.question.retention.RetentionAnalyze.{bootstrapServer, topic}
import com.venn.util.SimpleKafkaRecordDeserializationSchema
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.time.{Duration, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.state.StateTtlConfig.{StateVisibility, UpdateType}
import org.apache.flink.api.common.time.Time
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.metrics.Counter

object TestScalaClass {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置执行模式
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    env.enableCheckpointing(10000)
    env.setParallelism(1)
    env.setMaxParallelism(10)

    val timestampAssigner: WatermarkStrategy[Tuple3[String, String, Long]] = WatermarkStrategy
      .forBoundedOutOfOrderness[Tuple3[String, String, Long]](Duration.ofSeconds(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[Tuple3[String, String, Long]]() {
        override def extractTimestamp(t: (String, String, Long), l: Long): Long = t._3
      })
      .withIdleness(Duration.ofSeconds(10))

    val kafkaSource = KafkaSource
      .builder[KafkaSimpleStringRecord]()
      // stop job when consumer to latest offset ?
      //      .setBounded(OffsetsInitializer.latest())
      //      .setUnbounded(OffsetsInitializer.latest())
      .setBootstrapServers(bootstrapServer)
      .setGroupId("ra")
      .setTopics(topic)
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .build()


    val stream: DataStream[(String, String, Long)] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "xx")
      .map(new MapFunction[KafkaSimpleStringRecord, Tuple3[String, String, Long]]() {
        @throws[Exception]
        override def map(element: KafkaSimpleStringRecord): Tuple3[String, String, Long] = {

          val s = element.getValue
          val split: Array[String] = s.split(",")
          val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
          val parse: LocalDateTime = LocalDateTime.parse(split(2), dtf)
          val l: Long = parse.toInstant(ZoneOffset.ofHours(8)).toEpochMilli
          new Tuple3[String, String, Long](split(0), split(1), l)
        }
      }).assignTimestampsAndWatermarks(timestampAssigner)

    stream.keyBy((event: Tuple3[String, String, Long]) => event._1)
      .process(new KeyedProcessFunction[String, Tuple3[String, String, Long], Tuple3[String, Long, String]]() { //                    private MapState<String, ObjectOpenHashSet<String>> state;
        private var state: MapState[String, ObjectOpenHashSet[String]] = null
        private var flagState: ValueState[Int] = null
        var longCount: LongCounter = _

        @throws[Exception]
        override def open(parameters: Configuration): Unit = {

          val ttl = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.seconds(20))
            .setStateVisibility(StateVisibility.NeverReturnExpired)
            .setUpdateType(UpdateType.OnCreateAndWrite)
            .build()

          val myStateDesc: MapStateDescriptor[String, ObjectOpenHashSet[String]]
          = new MapStateDescriptor[String, ObjectOpenHashSet[String]]("myState", TypeInformation.of(new TypeHint[String]() {}), TypeInformation.of(new TypeHint[ObjectOpenHashSet[String]]() {}))
          myStateDesc.enableTimeToLive(ttl)
          state = getRuntimeContext.getMapState(myStateDesc)
          val flagStateDesc: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("flagState", TypeInformation.of(new TypeHint[Int]() {}))
          flagState = getRuntimeContext.getState(flagStateDesc)

          longCount = new LongCounter(1);

        }

        @throws[Exception]
        override def processElement(value: Tuple3[String, String, Long], context: KeyedProcessFunction[String, Tuple3[String, String, Long], Tuple3[String, Long, String]]#Context, collector: Collector[Tuple3[String, Long, String]]): Unit = {

          if ((longCount.getLocalValue % 10000) == 0) {
            state.clear()
          } else {
            val aidState = new ObjectOpenHashSet[String]
            aidState.add(value._2)
            state.put(value._1, aidState)

          }

        }

      }).addSink(new SinkFunction[Tuple3[String, Long, String]]() {
      @throws[Exception]
      override def invoke(value: Tuple3[String, Long, String], context: SinkFunction.Context): Unit = {
        System.out.println("result: " + value._1 + ", " + value._2 + ", " + value._3)
      }
    })

    env.execute("test timer")

  }

}
