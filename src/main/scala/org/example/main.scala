package org.example

import org.example.stream.streamApp

object main {

  def main(args: Array[String]): Unit = {
    val taskName = args(1)
    val taskType = args(0)
    taskType match {
      case "stream" => streamApp.run(taskName)
      case _ => throw new UnsupportedOperationException("")
    }
  }

}
