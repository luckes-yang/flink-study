package org.example.stream.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.slf4j.LoggerFactory

import java.util.concurrent.LinkedBlockingQueue

object ParallelSource {

  def runTask(env: StreamExecutionEnvironment): Unit = {
    val stream = env.addSource(new CustomParallelSource()).name("Parallel Source")
    stream.print()
    env.execute("parallel source with checkpoint")
  }

  /**
   * 对于多并行度source来说，无法保证数据的有序性
   */
  class CustomParallelSource extends ParallelSourceFunction[ChangeEvent] {
    private val LOG = LoggerFactory.getLogger(classOf[CustomParallelSource])
    private val queue = new LinkedBlockingQueue[ChangeEvent]()
    private val producer =  new ChangeEventProducer(queue, 0, 0)

    override def run(ctx: SourceFunction.SourceContext[ChangeEvent]): Unit = {
      producer.start()
      while (producer.isAlive) {
        val event = queue.poll()
        if (event != null) {
          ctx.collect(event)
        } else {
          LOG.info("没有数据，等待1秒")
          Thread.sleep(1000)
        }
      }
    }

    override def cancel(): Unit = {
      producer.close()
    }
  }
}
