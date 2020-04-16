package com.wsk.bigdata.dynamicconfig

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * Broadcast Stream 实现动态配置
  */
object BroadcastStreamApp {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置整个环境的并行度
    env.setParallelism(6)

    env.addSource(new ConfigSource())
  }

}


class ConfigSource extends RichParallelSourceFunction[String]{

  var  isRun = true
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

  }

  override def cancel(): Unit = {

  }
}
