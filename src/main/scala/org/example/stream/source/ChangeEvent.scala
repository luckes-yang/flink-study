package org.example.stream.source

import java.sql.Timestamp

/**
 * 模拟数据事件
 *
 * @param subTask 子任务ID
 * @param index   事件偏移量，主要记录事件位置
 * @param time    事件事件，记录事件生成事件
 */
case class ChangeEvent(subTask: Int, index: Int, time: Timestamp)
