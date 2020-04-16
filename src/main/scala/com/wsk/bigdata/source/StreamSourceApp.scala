package com.wsk.bigdata.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._


/**
  * DataStream Source 编程
  */
object StreamSourceApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    fromCollection(env)

    fromTextFile(env)

    env.execute("SourceApp")
  }

  /**
    *从 集合中生成 DataStream
    * @param env
    */
  def fromCollection(env: StreamExecutionEnvironment): Unit ={
    val nums: DataStream[Int] = env.fromCollection(1 to 10)
    nums.map(_+1).print().setParallelism(1)
  }

  /**
    * 从text文件（夹）中生成 DataStream
    *
    * @param env
    */
  def fromTextFile(env: StreamExecutionEnvironment): Unit ={
    // 读取文件
//    val texts = env.readTextFile(" data/input/source/text/hello.txt")
    //读取文件夹
    val texts = env.readTextFile(" data/input/source/text")
    texts.print().setParallelism(1)
  }

}
