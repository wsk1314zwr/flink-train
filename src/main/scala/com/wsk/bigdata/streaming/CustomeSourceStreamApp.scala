package com.wsk.bigdata.streaming

import com.wsk.bigdata.source.custome.{CustomeParallelSource, CustomeRichParallelSource, CustomeSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object CustomeSourceStreamApp {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val text = env.addSource(new CustomeSource()).setParallelism(1)
//    val text = env.addSource(new CustomeParallelSource).setParallelism(2)
    val text = env.addSource(new CustomeRichParallelSource()).setParallelism(2)
    text.map(x => {
      println("CustomeSourceStreamApp接收到数据：" + x)
      x
    }).print()
//      .timeWindowAll(Time.seconds(4), Time.seconds(2))
//      .sum(0)
//      .print()
//      .setParallelism(1)

    env.execute("CustomeSourceStreamApp")
//    println(env.getExecutionPlan)

  }

}
