package org.example.stream.source

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWordCount {

  /**
   * 使用socket流的方式实现word count
   * socket 启动： nc -lk 9998 -v
   * 部分linux主机需要使用 ncat -lk 9998 -v
   * @param env streaming 环境上下文
   */
  def runTask(env: StreamExecutionEnvironment): Unit = {
    val host = "192.168.1.200"
    val port = 9998

    val text = env.socketTextStream(host, port)
    val count = text.flatMap(_.split("\\W+"))
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    count.print()

    env.execute("Socket Word Count")
  }

}
