package org.example.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object streamApp {

  def run(taskName: String): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    taskName match {
      case "WordCount" =>
    }
  }

}
