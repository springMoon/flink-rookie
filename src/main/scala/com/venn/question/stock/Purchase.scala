package com.venn.question.stock

import com.google.gson.JsonParser
import com.venn.entity.KafkaSimpleStringRecord
import com.venn.flink.asyncio.AsyncFunctionForMysqlJava
import com.venn.question.stock.entry.{OverStock, OverStockDetail}
import com.venn.question.stock.util.StockCommon
import com.venn.util.{DateTimeUtil, SimpleKafkaRecordDeserializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit


/**
 * @Classname DataDispatcher
 * @Description 1. purchase
 *              step:
 *    1. join main & detail
 *       2. join dim
 *       3. sum by day & agency & product
 * @Date 2023/6/12
 * @Created by venn
 */
object Purchase {

  val LOG = LoggerFactory.getLogger("com.venn.question.stock.Purchase")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.getConfig.setAutoWatermarkInterval(-1)

    //    CheckpointUtil.setCheckpoint(env, "rocksdb", "file:///tmp/flink-checkpoint", 10 * 1000, 20 * 1000)

    // current 0 点
    val currentLong = System.currentTimeMillis()
    val str = DateTimeUtil.formatMillis(currentLong, DateTimeUtil.YYYY_MM_DD)
    val currentZero = DateTimeUtil.parse(str).getTime

    //source
    val kafkaSourceOverStock = KafkaSource.builder[KafkaSimpleStringRecord]()
      .setBootstrapServers(StockCommon.KAFKA_BOOTSTRAT_SERVER)
      .setTopics("db_" + StockCommon.OVERSTOCK)
      .setGroupId(StockCommon.OVERSTOCK)
      .setClientIdPrefix(StockCommon.OVERSTOCK)
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .setStartingOffsets(OffsetsInitializer.timestamp(currentZero))
      .build()

    //source
    val kafkaSourceOverStockDetail = KafkaSource.builder[KafkaSimpleStringRecord]()
      .setBootstrapServers(StockCommon.KAFKA_BOOTSTRAT_SERVER)
      .setTopics("db_" + StockCommon.OVERSTOCK_DETAIL)
      .setGroupId(StockCommon.OVERSTOCK_DETAIL)
      .setClientIdPrefix(StockCommon.OVERSTOCK_DETAIL)
      .setDeserializer(new SimpleKafkaRecordDeserializationSchema())
      .setStartingOffsets(OffsetsInitializer.timestamp(currentZero))
      .build()


    val sourceOverStock = env.fromSource(kafkaSourceOverStock, WatermarkStrategy.noWatermarks(), "overStock")
    val sourceOverStockDetail = env.fromSource(kafkaSourceOverStockDetail, WatermarkStrategy.noWatermarks(), "overStockDetail")



    val overStream = sourceOverStock.flatMap(new RichFlatMapFunction[KafkaSimpleStringRecord, OverStock] {

      var jsonParser: JsonParser = _

      override def open(parameters: Configuration): Unit = {
        jsonParser = new JsonParser
      }

      override def flatMap(element: KafkaSimpleStringRecord, collector: Collector[OverStock]): Unit = {

        val json = jsonParser.parse(element.getValue).getAsJsonObject

        // todo remove delete operator
        if ("d".equals(json.get("operator_type"))) {
          return
        }
        val after = json.get("after").getAsJsonObject

        var id: Int = 1
        if (after.has("id")) {
          id = after.get("id").getAsInt
        }
        var fdateStr = ""
        if (after.has("fdate")) {
          fdateStr = after.get("fdate").getAsString
        }
        var fid = ""
        if (after.has("fid")) {
          fid = after.get("fid").getAsString
        }
        var fbillNo = ""
        if (after.has("fbillno")) {
          fbillNo = after.get("fbillno").getAsString
        }
        var fcustomerid = ""
        if (after.has("fcustomerid")) {
          fcustomerid = after.get("fcustomerid").getAsString
        }
        var f_gjzh = ""
        if (after.has("f_gjzh")) {
          f_gjzh = after.get("f_gjzh").getAsString
        }

        val fdate = DateTimeUtil.parse(fdateStr).getTime

        val overStock = new OverStock(id, fdate, fid, fbillNo, fcustomerid, f_gjzh)
        collector.collect( overStock)
      }
    })
      .name("overStockFlatMap")
      .uid("overStockFlatMap")


    val detailStream = sourceOverStockDetail.flatMap(new RichFlatMapFunction[KafkaSimpleStringRecord, OverStockDetail] {

      var jsonParser: JsonParser = _

      override def open(parameters: Configuration): Unit = {
        jsonParser = new JsonParser
      }

      override def flatMap(element: KafkaSimpleStringRecord, collector: Collector[OverStockDetail]): Unit = {

        val json = jsonParser.parse(element.getValue).getAsJsonObject

        // todo remove delete operator
        if ("d".equals(json.get("operator_type"))) {
          return
        }
        val after = json.get("after").getAsJsonObject

        var id = 0
        if (after.has("id")) {
          id = after.get("id").getAsInt
        }
        var fid = ""
        if (after.has("fid")) {
          fid = after.get("fid").getAsString
        }
        var fentryId = ""
        if (after.has("fentryid")) {
          fentryId = after.get("fentryid").getAsString
        }
        var fmaterialid = ""
        if (after.has("fmaterialid")) {
          fmaterialid = after.get("fmaterialid").getAsString
        }
        var frealQtyStr = "0.0"
        if (after.has("frealqty")) {
          frealQtyStr = after.get("frealqty").getAsString
        }
        val frealQty = new java.math.BigDecimal(frealQtyStr)

        val overStockDetail = new OverStockDetail(id, fid, fentryId, fmaterialid, frealQty)
        collector.collect(overStockDetail)
      }
    })
      .name("detailFlatMap")
      .uid("detailFlatMap")

    // todo async
//    val asyncRedisFunction = new AsyncFunctionForMysqlJava(uri)
//    val overAsyncStream = AsyncDataStream.unorderedWait(overStream, asyncRedisFunction, 5L, TimeUnit.SECONDS, 100)


    overStream
      .keyBy(_.getFid)
      .intervalJoin(detailStream.keyBy(_.getFid))
      .between(Time.seconds(-2), Time.seconds(2))
      .lowerBoundExclusive()
      .process(new ProcessJoinFunction[OverStock, OverStockDetail, String] {

        override def open(parameters: Configuration): Unit = {
          // init dim data
          // init ods_poc_k3_customer & dim_agency_doc
          val sql1 = ""

          // init ods_poc_k3_material & ods_poc_dim_product_doc_api


        }

        override def processElement(over: OverStock, detail: OverStockDetail, context: ProcessJoinFunction[OverStock, OverStockDetail, String]#Context, collector: Collector[String]): Unit = {





        }


      })


    env.execute("Purchase")

  }
}
