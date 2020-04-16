package com.wsk.bigdata

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConversions._

/**
  * DataSet 普通的广播变量
  */
object BroadCastApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val broadFCastDs = env.fromElements("a", "b", "c")

    val data = env.fromElements(1, 2, 3).setParallelism(2)

    data.map(new RichMapFunction[Int, Int] {
      var broadcastSet: Traversable[String] = null

      override def open(parameters: Configuration): Unit = {
        broadcastSet = getRuntimeContext.getBroadcastVariable[String]("mybroadCast")
        for (s <- broadcastSet) {
          println("广播变量值：" + s)
        }
      }

      override def map(value: Int): Int = {
        value
      }
    }).withBroadcastSet(broadFCastDs, "mybroadCast")
      .print()
  }
}
