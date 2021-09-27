package com.venn.util

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckpointUtil {

  def setCheckpoint(env: StreamExecutionEnvironment, stateBackendStr: String, checkpointPath: String, interval: Long, timeOut: Long) = {
    var stateBackend: StateBackend = null
    if ("rocksdb".equals(stateBackendStr)) {
      stateBackend = new EmbeddedRocksDBStateBackend(true)
    } else {
      stateBackend = new HashMapStateBackend()
    }
    env.setStateBackend(stateBackend)
    // checkpoint
    env.enableCheckpointing(interval * 1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(timeOut * 1000)
    // Flink 1.11.0 new feature: Enables unaligned checkpoints
    env.getCheckpointConfig.enableUnalignedCheckpoints()
    // checkpoint dir
    env.getCheckpointConfig.setCheckpointStorage(checkpointPath)

  }

  /**
   *
   * @param env
   * @param stateBackendStr state backend: rocksdb, other
   * @param checkpointPath  checkpoint path
   * @param interval        second
   */
  def setCheckpoint(env: StreamExecutionEnvironment, stateBackendStr: String, checkpointPath: String, interval: Long) = {
    var stateBackend: StateBackend = null
    if ("rocksdb".equals(stateBackendStr)) {
      stateBackend = new EmbeddedRocksDBStateBackend(true)
    } else {
      stateBackend = new HashMapStateBackend()
    }
    env.setStateBackend(stateBackend)
    // checkpoint
    env.enableCheckpointing(interval * 1000, CheckpointingMode.EXACTLY_ONCE)
    //    env.getCheckpointConfig.setCheckpointTimeout(timeOut * 1000)
    // Flink 1.11.0 new feature: Enables unaligned checkpoints
    env.getCheckpointConfig.enableUnalignedCheckpoints()
    // checkpoint dir
    env.getCheckpointConfig.setCheckpointStorage(checkpointPath)

  }

}
