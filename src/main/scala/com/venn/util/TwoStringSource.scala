package com.venn.util

import org.apache.flink.streaming.api.functions.source.SourceFunction

class TwoStringSource extends SourceFunction[String] {

  var flag = true

  override def cancel(): Unit = {

    flag = false
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    while (flag) {
      val str = MathUtil.getRadomNum(1)
      ctx.collect(str + "," + StringUtil.getRandomString(1).toUpperCase)
      Thread.sleep(1000)
    }
  }
}
