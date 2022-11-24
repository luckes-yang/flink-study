package org.example.stream

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import java.sql.Timestamp
import java.time.LocalDateTime

object CustomSingleSource {

  def runTask(env:StreamExecutionEnvironment): Unit ={
    val stream = env.addSource(new SingleSource())
    stream.print()

    env.execute("single source")
  }


  /**
   * RichSourceFunction支撑Source全生命周期控制
   */
  class SingleSource extends SourceFunction[String] {
    private var running = true

    override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
      while (running){
        val now = LocalDateTime.now()
        sourceContext.collect(Timestamp.valueOf(now).toString)
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

}
