package org.example.stream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.slf4j.LoggerFactory

import java.util.concurrent.LinkedBlockingQueue

object CustomSingleSource {

  def runTask(env:StreamExecutionEnvironment): Unit ={
    val stream = env.addSource(new SingleSource())
    stream.print()

    env.execute("single source")
  }


  /**
   * RichSourceFunction支撑Source全生命周期控制
   */
  class SingleSource extends SourceFunction[ChangeEvent] {
    private val queue = new LinkedBlockingQueue[ChangeEvent]()
    private val producer = new ChangeEventProducer(queue, 0, 0)
    override def run(sourceContext: SourceFunction.SourceContext[ChangeEvent]): Unit = {
      producer.start()
      while (producer.isAlive) {
        val event = queue.poll()
        if (event != null) {
          sourceContext.collect(event)
        } else {
          Thread.sleep(1000)
        }
      }
    }

    override def cancel(): Unit = {
      producer.close()
    }
  }
}
