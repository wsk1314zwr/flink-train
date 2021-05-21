package com.wsk.bigdata

import com.wsk.bigdata.cdnproject.PKMySQLSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * 测试 MYsql数据源
  */
object PKMySQLSourceTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val mysqlSource = env.addSource(new PKMySQLSource()).setParallelism(1)
    mysqlSource.print().setParallelism(1)

    env.execute("PKMySQLSourceTest")
  }

}

