package org.example.stream

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{IntegerTypeInfo, PrimitiveArrayTypeInfo}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.collection.JavaConverters._

object SingleSourceWithCheckpoint {

  def runTask(env: StreamExecutionEnvironment): Unit = {
    val checkpoint = "file:///opt/flink/checkpoint/SingleSourceWithCheckpoint"
    env.getCheckpointConfig.setCheckpointStorage(checkpoint)
    env.enableCheckpointing(60000L)
    val stream = env.addSource(new SingleSource())
    stream.print()
    env.execute("single source with checkpoint")
  }


  class SingleSource extends SourceFunction[Event] with CheckpointedFunction{
    private val LOG = LoggerFactory.getLogger(classOf[SingleSource])
    private var running = true
    private var offsetState:ListState[Array[Int]] = _
    private val OFFSETS_STATE_NAME = "offsets-state"
    @transient @volatile private var restoreOffsetState: Array[Int] = _

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      this.offsetState.clear()
      this.offsetState.add(restoreOffsetState)
      LOG.info(s"save position: ${restoreOffsetState.head}")
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      val stateStore = context.getOperatorStateStore
      this.offsetState = stateStore.getUnionListState(new ListStateDescriptor[Array[Int]](OFFSETS_STATE_NAME, PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO))
      if(context.isRestored){
        this.offsetState.get().asScala.foreach(offset => {
          restoreOffsetState = offset
          LOG.info(s"restart from position: ${restoreOffsetState.head}")
        })
      } else {
        restoreOffsetState = Array(0)
        LOG.info(s"start from init position: ${restoreOffsetState.head}")
      }
    }

    override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
      while (running){
        val now = LocalDateTime.now()
        val event = Event(restoreOffsetState.head, Timestamp.valueOf(now))
        ctx.collect(event)
        restoreOffsetState(0) += 1
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  case class Event(index:Int, time: Timestamp)
}
