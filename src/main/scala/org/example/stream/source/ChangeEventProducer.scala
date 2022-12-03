package org.example.stream.source

import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util

/**
 * 模拟一个数据生产者，生成ChangeEvent事件
 *
 * @param queue   生产队列
 * @param subTask 子任务号
 * @param index   起始位置
 */
class ChangeEventProducer(queue: util.Queue[ChangeEvent], subTask: Int, index: Int) extends Thread {
  private val LOG = LoggerFactory.getLogger(classOf[ChangeEventProducer])
  private val defaultTime = 1000L
  private var isRunning = true
  private var sleepTime: Long = defaultTime

  override def run(): Unit = {
    LOG.info("启动数据生产者：{}, 开始位置：{}", subTask, index)
    var i = index
    while (isRunning) {
      val event = ChangeEvent(subTask, index, Timestamp.valueOf(LocalDateTime.now()))
      queue.add(event)
      i += 1
      Thread.sleep(sleepTime)
    }
  }

  def close(): Unit = {
    LOG.info("关闭数据生产者")
    this.isRunning = false
  }

  def pause(): Unit = {
    LOG.info("暂停数据生产者")
    this.sleepTime = Long.MaxValue
  }

  def wakeup(): Unit = {
    LOG.info("恢复数据生产者")
    this.sleepTime = this.defaultTime
  }
}
