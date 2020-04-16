package com.wsk.bigdata.streaming.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object WindowReduceFuncApp {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 默认的Time 是 ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val stream: DataStream[String] = env.socketTextStream("10.199.140.144", 9099)
    stream.flatMap(_.split(","))
      .map(x => (1, x.toInt))
      .keyBy(0) // 输入的数据是格式是 1,2,3,4,5,6 ,都聚合到一个分区去，测试效果
      .timeWindow(Time.seconds(4)) //Time 滚动窗口
      .reduce((v1, v2) => {
      println(v1 + "-------" + v2)
      (v1._1, v1._2 + v2._2)
    })
      .print().setParallelism(1)

    env.execute("windowApp")


  }
}
