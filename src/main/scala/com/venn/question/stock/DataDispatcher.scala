package com.venn.question.stock

import com.google.gson.JsonParser
import com.venn.question.stock.util.StockCommon
import com.venn.source.mysql.cdc.CommonStringDebeziumDeserializationSchema
import com.venn.util.DateTimeUtil
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import java.util.Properties

/**
 * @Classname DataDispatcher
 * @Description 1. data dispatcher record
 * @Date 2023/6/8
 * @Created by venn
 */
object DataDispatcher {

  val LOG = LoggerFactory.getLogger("com.venn.question.stock.DataDispatcher")

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    CheckpointUtil.setCheckpoint(env, "rocksdb", "file:///tmp/flink-checkpoint", 10 * 1000, 20 * 1000)

    // current 0 点
    val currentLong = System.currentTimeMillis()
    val str = DateTimeUtil.formatMillis(currentLong, DateTimeUtil.YYYY_MM_DD)
    val currentZero = DateTimeUtil.parse(str).getTime

    //source
    val cdcSource = MySqlSource.builder[String]()
      .hostname(StockCommon.MYSQL_HOST)
      .port(StockCommon.MYSQL_PORT)
      .username(StockCommon.MYSQL_USER)
      .password(StockCommon.MYSQL_PASS)
      .databaseList("bi_poc")
      .tableList("bi_poc.ods_poc_sfa_distributoroutstocklist", "bi_poc.ods_poc_sfa_distributoroutstocklist_detail", "bi_poc.ods_poc_k3_sal_outstock", "bi_poc.ods_poc_k3_sal_outstockentry")
      //      .tableList("bi_poc.ods_poc_k3_sal_outstockentry")
      //      .startupOptions(StartupOptions.timestamp(currentZero))
      .startupOptions(StartupOptions.initial())
      .deserializer(new CommonStringDebeziumDeserializationSchema(StockCommon.MYSQL_HOST, StockCommon.MYSQL_PORT))
      .build()

    val source = env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "cdc")


    source.addSink(new RichSinkFunction[String] {

      var producer: KafkaProducer[String, String] = _
      var jsonParser: JsonParser = _

      override def open(parameters: Configuration): Unit = {

        val prop = new Properties
        prop.put("bootstrap.servers", StockCommon.KAFKA_BOOTSTRAT_SERVER)
        prop.put("request.required.acks", "-1")
        //prop.put("auto.offset.reset", "latest");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        prop.put("request.timeout.ms", "20")

        producer = new KafkaProducer(prop)

        jsonParser = new JsonParser

      }

      override def invoke(element: String, context: SinkFunction.Context): Unit = {

        val json = jsonParser.parse(element).getAsJsonObject
        val db = json.get("db").getAsString
        val table = json.get("table").getAsString

        val record = new ProducerRecord[String, String](db + "_" + table, element)

        producer.send(record)

      }
    })



    //    source.flatMap(new RichFlatMapFunction[String, (String, Object)] {
    //
    //      var jsonParser: JsonParser = _
    //
    //      override def open(parameters: Configuration): Unit = {
    //        jsonParser = new JsonParser
    //      }
    //
    //      override def flatMap(element: String, collector: Collector[(String, Object)]): Unit = {
    //
    //        val json = jsonParser.parse(element).getAsJsonObject
    //
    //        // todo remove delete operator
    //        if ("d".equals(json.get("operator_type"))) {
    //          return
    //        }
    //        val after = json.get("after").getAsJsonObject
    //        val table = json.get("table").getAsString
    //        table match {
    //          case PurchaseCommon.OVERSTOCK =>
    //
    //            var id: Int = 1
    //            if (after.has("id")) {
    //              id = after.get("id").getAsInt
    //            }
    //            var fdateStr = ""
    //            if (after.has("fdate")) {
    //              fdateStr = after.get("fdate").getAsString
    //            }
    //            var fid = ""
    //            if (after.has("fid")) {
    //              fid = after.get("fid").getAsString
    //            }
    //            var fbillNo = ""
    //            if (after.has("fbillno")) {
    //              fbillNo = after.get("fbillno").getAsString
    //            }
    //            var fcustomerid = ""
    //            if (after.has("fcustomerid")) {
    //              fcustomerid = after.get("fcustomerid").getAsString
    //            }
    //            var f_gjzh = ""
    //            if (after.has("f_gjzh")) {
    //              f_gjzh = after.get("f_gjzh").getAsString
    //            }
    //
    //            val fdate = DateTimeUtil.parse(fdateStr).getTime
    //
    //            val overStock = new OverStock(id, fdate, fid, fbillNo, fcustomerid, f_gjzh)
    //            collector.collect((table, overStock))
    //          case PurchaseCommon.OVERSTOCK_DETAIL =>
    //
    //            var id = 0
    //            if (after.has("id")) {
    //              id = after.get("id").getAsInt
    //            }
    //            var fid = ""
    //            if (after.has("fid")) {
    //              fid = after.get("fid").getAsString
    //            }
    //            var fentryId = ""
    //            if (after.has("fentryid")) {
    //              fentryId = after.get("fentryid").getAsString
    //            }
    //            var fmaterialid = ""
    //            if (after.has("fmaterialid")) {
    //              fmaterialid = after.get("fmaterialid").getAsString
    //            }
    //            var frealQtyStr = "0.0"
    //            if (after.has("frealqty")) {
    //              frealQtyStr = after.get("frealqty").getAsString
    //            }
    //            val frealQty = new java.math.BigDecimal(frealQtyStr)
    //
    //            val overStockDetail = new OverStockDetail(id, fid, fentryId, fmaterialid, frealQty)
    //            collector.collect((table, overStockDetail))
    //          case PurchaseCommon.STOCK_LIST =>
    //          case PurchaseCommon.STOCK_LIST_DETAIL =>
    //          case _ =>
    //            LOG.debug("ignore update: ", element)
    //        }
    //
    //
    //      }
    //    })
    //      .print()


    env.execute("moveSale")

  }
}
