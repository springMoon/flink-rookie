package com.venn.stream.api.filesink

import java.io.IOException
import java.nio.charset.StandardCharsets
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner

class DayBucketAssigner extends BucketAssigner[ObjectNode, String] {

  /**
    * bucketId is the output path
    * @param element
    * @param context
    * @return
    */
  override def getBucketId(element: ObjectNode, context: BucketAssigner.Context): String = {
    //context.currentProcessingTime()
    val day = element.get("date").asText("19790101000000").substring(0, 8)
    // wrap can day + "/" + xxx
    day
  }

  override def getSerializer: SimpleVersionedSerializer[String] = {

    StringSerializer
  }

  /**
    * 实现参考 ： org.apache.flink.runtime.checkpoint.StringSerializer
    */
  object StringSerializer extends SimpleVersionedSerializer[String] {
    val VERSION = 77

    override def getVersion = 77

    @throws[IOException]
    override def serialize(checkpointData: String): Array[Byte] = checkpointData.getBytes(StandardCharsets.UTF_8)

    @throws[IOException]
    override def deserialize(version: Int, serialized: Array[Byte]): String = if (version != 77) throw new IOException("version mismatch")
    else new String(serialized, StandardCharsets.UTF_8)
  }


}


