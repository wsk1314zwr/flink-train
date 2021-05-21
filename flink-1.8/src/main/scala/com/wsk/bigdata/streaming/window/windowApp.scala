package com.wsk.bigdata.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
/**
  * Window ReduceFunc 操作 App
  *
  */
object windowApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 默认的Time 是 ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val stream: DataStream[String] = env.socketTextStream("10.199.140.144",9099)
    stream.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      //      .countWindow(100) // count window
      //      .timeWindow(Time.seconds(4),Time.seconds(2)) // Time 滑动窗口
      .timeWindow(Time.seconds(4)) //Time 滚动窗口
      .sum(1)
      .print().setParallelism(1)

    env.execute("windowApp")

  }
}
