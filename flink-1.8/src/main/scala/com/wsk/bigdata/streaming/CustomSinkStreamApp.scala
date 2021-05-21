package com.wsk.bigdata.streaming

import com.wsk.bigdata.sink.custome.{CustomStreamSinkToMysql, Student}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CustomSinkStreamApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("10.199.140.144", 9099)
    val stuStream: DataStream[Student] = stream.map(x => {
      val fields: Array[String] = x.split(",")
      Student(fields(0).toInt, fields(1), fields(2).toInt)
    })
    stuStream.addSink(new CustomStreamSinkToMysql()).setParallelism(1)

    env.execute("CustomSinkStreamApp")
  }
}
