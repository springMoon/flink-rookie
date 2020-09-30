package com.venn.connector.filesink

import java.io.File
import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer
import org.apache.hadoop.fs.Path


/**
  * 根据实际数据返回数据输出的路径
  */
class DayBasePathBucketer extends BasePathBucketer[String]{

  /**
    * 返回路径
    * @param clock
    * @param basePath
    * @param element
    * @return
    */
  override def getBucketPath(clock: Clock, basePath: Path, element: String): Path = {
    // yyyyMMdd
    val day = element.substring(1, 9)
    new Path(basePath + File.separator + day)
  }
}
