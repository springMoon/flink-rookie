package com.venn.connector.filesink

import java.io.File
import java.nio.charset.StandardCharsets
import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.core.fs.FSDataOutputStream
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.util.Preconditions

/**
  * 实现参考 ： org.apache.flink.streaming.api.functions.sink.filesystem.BulkWriterTest
  */
class DayBulkWriter extends BulkWriter[ObjectNode] {

  val charset = StandardCharsets.UTF_8
  var stream: FSDataOutputStream = _

  def DayBulkWriter(inputStream: FSDataOutputStream): DayBulkWriter = {
    stream = Preconditions.checkNotNull(inputStream);
    this
  }

  /**
    * write element
    *
    * @param element
    */
  override def addElement(element: ObjectNode): Unit = {
    this.stream.write(element.toString.getBytes(charset))
    // wrap
    this.stream.write('\n')

  }

  override def flush(): Unit = {
    this.stream.flush()
  }

  /**
    * output stream  is input parameter, just flush, close is factory's job
    */
  override def finish(): Unit = {
    this.flush()
  }

}

/**
  * 实现参考 ： org.apache.flink.streaming.api.functions.sink.filesystem.BulkWriterTest.TestBulkWriterFactory
  */
class DayBulkWriterFactory extends BulkWriter.Factory[ObjectNode] {
  override def create(out: FSDataOutputStream): BulkWriter[ObjectNode] = {
    val dayBulkWriter = new DayBulkWriter
    dayBulkWriter.DayBulkWriter(out)

  }
}
