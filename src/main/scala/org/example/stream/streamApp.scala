package org.example.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object streamApp {

  def run(taskName: String): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    taskName match {
      case "WordCount" => SocketWordCount.runTask(env)
      case "SingleSource" => CustomSingleSource.runTask(env)
      case "SingleSourceWithCheckpoint" => SingleSourceWithCheckpoint.runTask(env)
      case _ => throw new IllegalArgumentException("unknown stream task")
    }
  }

}
