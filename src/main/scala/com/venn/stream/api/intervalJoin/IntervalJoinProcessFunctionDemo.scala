package com.venn.stream.api.intervalJoin

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector

/**
  *
  */
class IntervalJoinProcessFunctionDemo extends ProcessJoinFunction[IntervalUser, IntervalUser, IntervalUser] {

  override def open(parameters: Configuration): Unit = {

  }


  override def processElement(left: IntervalUser,
                              right: IntervalUser,
                              ctx: ProcessJoinFunction[IntervalUser, IntervalUser, IntervalUser]#Context,
                              out: Collector[IntervalUser]): Unit = {

//    println("left timestamp : " + ctx.getLeftTimestamp)
//    println("right timestamp : " + ctx.getRightTimestamp)

    out.collect(IntervalUser(left.id , left.name, right.phone, (left.date + "-" + right.date)))

  }



  override def close(): Unit = {

  }

}
