package org.example.stream.source

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.slf4j.LoggerFactory

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.JavaConverters._

object SingleSourceWithCheckpoint {

  def runTask(env: StreamExecutionEnvironment): Unit = {
    val checkpoint = "file:///opt/flink/checkpoint/SingleSourceWithCheckpoint"
    env.getCheckpointConfig.setCheckpointStorage(checkpoint)
    env.enableCheckpointing(60000L)
    val stream = env.addSource(new SingleSource()).name("Single Source")
    stream.addSink(new SingleSink()).name("SingleSink")
    env.execute("single source with checkpoint")
  }


  /**
   * RichSourceFunction 比 SourceFunction 多了open\close等生命周期的管理
   */
  class SingleSource extends SourceFunction[ChangeEvent] with CheckpointedFunction {
    private val LOG = LoggerFactory.getLogger(classOf[SingleSource])
    private var offsetState: ListState[Array[Int]] = _
    private val OFFSETS_STATE_NAME = "offsets-state"
    @transient
    @volatile private var restoreOffsetState: Array[Int] = _
    private val queue = new LinkedBlockingQueue[ChangeEvent]()
    private var producer: ChangeEventProducer = _

    /**
     * 保存checkpoint
     */
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      this.offsetState.clear()
      this.offsetState.add(restoreOffsetState)
      LOG.info("保存检查点位置: {}", restoreOffsetState.head)
    }

    /**
     * 初始化checkpoint
     * 若存在checkpoint信息，则从checkpoint点进行恢复
     */
    override def initializeState(context: FunctionInitializationContext): Unit = {
      val stateStore = context.getOperatorStateStore
      this.offsetState = stateStore.getUnionListState(new ListStateDescriptor[Array[Int]](OFFSETS_STATE_NAME, PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO))
      if (context.isRestored) {
        this.offsetState.get().asScala.foreach(offset => {
          restoreOffsetState = offset
          LOG.info("从指定位置重启: {}", restoreOffsetState.head)
        })
      } else {
        restoreOffsetState = Array(0)
        LOG.info(s"初始化启动位置:{}", restoreOffsetState.head)
      }
      this.producer = new ChangeEventProducer(queue, 0, restoreOffsetState.head)
    }

    /**
     * 当生产线程是存活状态，持续从队列拉取数据
     * 若队列中没有数据，则等待2s后再拉取数据
     */
    override def run(ctx: SourceFunction.SourceContext[ChangeEvent]): Unit = {
      producer.start()
      while (producer.isAlive) {
        val event = queue.poll()
        if (event != null) {
          ctx.collect(event)
          this.restoreOffsetState(0) += 1
        } else {
          LOG.info("没有数据，等待2秒")
          Thread.sleep(2000)
        }
      }
    }

    override def cancel(): Unit = {
      producer.close()
    }
  }

  class SingleSink extends SinkFunction[ChangeEvent] {
    override def invoke(value: ChangeEvent, context: SinkFunction.Context): Unit = {
      println(value)
    }
  }
}
