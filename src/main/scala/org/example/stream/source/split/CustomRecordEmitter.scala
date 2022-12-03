package org.example.stream.source.split

import org.apache.flink.api.connector.source.SourceOutput
import org.apache.flink.connector.base.source.reader.RecordEmitter
import org.example.stream.source.ChangeEvents

/**
 * 这个类的主要作用和SourceContext.collect类似，将数据发送到集群
 * 这里涉及两个主要泛型
 * RecordEmitter[E,T,C]: E表示原始的数据类型，T表示转换后提交到集群的类型
 * 注意：类型T必须可以被序列化
 * 这里演示不需要类型转换，因此，原始类型和提交类型一致
 * @tparam T
 */
class CustomRecordEmitter[T] extends RecordEmitter[ChangeEvents[T], T, CustomCheckpoint]{
  override def emitRecord(element: ChangeEvents[T], output: SourceOutput[T], splitState: CustomCheckpoint): Unit = {
    element.events.foreach(e => {
      output.collect(e)
    })
  }
}
