package com.venn.question.stock.util

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

/**
 * @Classname OverStockFlatMapFunction
 * @Description TODO
 * @Date 2023/6/13
 * @Created by venn
 */
class OverStockFlatMapFunction extends FlatMapFunction[String, String]{
  override def flatMap(t: String, collector: Collector[String]): Unit = {

  }
}
