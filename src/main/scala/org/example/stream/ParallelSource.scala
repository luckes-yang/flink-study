package org.example.stream

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import java.sql.Timestamp
import java.time.LocalDateTime

object ParallelSource {

  def runTask(env: StreamExecutionEnvironment): Unit = {
    val stream = env.addSource(new CustomParallelSource())
    stream.print()
    env.execute("parallel source with checkpoint")
  }


  class CustomParallelSource extends ParallelSourceFunction[Event] {
    private var running = true
    private var index = 0
    private val sourceNum = this.hashCode()

    override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
      while (running){
        val event = Event(sourceNum, index, Timestamp.valueOf(LocalDateTime.now()))
        ctx.collect(event)
        index += 1
        Thread.sleep(1000L)
      }
    }

    override def cancel(): Unit = {
      this.running = false
    }
  }

  case class Event(hash:Int, index: Int, time: Timestamp)

}
