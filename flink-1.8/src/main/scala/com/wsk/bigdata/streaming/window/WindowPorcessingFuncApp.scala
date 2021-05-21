package com.wsk.bigdata.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Windows的Porcessing Function操作
  */
object WindowPorcessingFuncApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 默认的Time 是 ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val stream: DataStream[String] = env.socketTextStream("10.199.140.144", 9099)
    stream.flatMap(_.split(","))
      .map(x => (1, x.toInt))
      .keyBy(_._1) // 输入的数据是格式是 1,2,3,4,5,6 ,都聚合到一个分区去，测试效果
      .timeWindow(Time.seconds(4)) //Time 滚动窗口
      .process(new ProcessWindowFunction[(Int, Int), String, Int, TimeWindow]() {
      override def process(key: Int, context: Context, elements: Iterable[(Int, Int)], out: Collector[String]): Unit = {
        var count = 0L
        for (in <- elements) {
          count = count + 1
        }
        out.collect(s"Window ${context.window} count: $count")
      }
    })
      .print()
      .setParallelism(1)

    env.execute("WindowPorcessingFuncApp")

  }

//class MyProcessWindowFunction extends ProcessWindowFunction[(Int, Int), String, String, TimeWindow] {
//  override def process(key: String, context: Context, elements: Iterable[(Int, Int)], out: Collector[String]): Unit = {
//    var count = 0L
//    for (in <- elements) {
//      count = count + 1
//    }
//    out.collect(s"Window ${context.window} count: $count")
//  }
}