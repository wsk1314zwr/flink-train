package com.wsk.bigdata.table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
  * @Description:
  * @Auther: wsk
  * @Date: 2019/9/30 13:22
  * @Version: 1.0
  */
object StreamSQLFloatApp {

  def main(args: Array[String]): Unit= {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tenv = StreamTableEnvironment.create(env)

    val stream: DataStream[Float] = env.fromElements(99892.98f, 99892.98f)
//    val stream: DataStream[Float] = env.fromElements(1.00f, 1.00f)
    tenv.registerDataStream("test",stream,'price)

    val table = tenv.sqlQuery("select sum(price) from test")
    table.printSchema()
    val appdendStrem = tenv.toRetractStream[Row](table)
    appdendStrem.print()
    env.execute()
  }

}
