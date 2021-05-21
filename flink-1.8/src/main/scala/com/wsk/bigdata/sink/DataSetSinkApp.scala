package com.wsk.bigdata.sink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.table.sinks.CsvTableSink

/**
  * DataSet sink编程
  *
  * 内置的sink：
  * writeAsText
  * writeAsCsv
  * print
  * write
  * output
  *
  * 生产中肯定是需要自定义sink的
  *
  */
object DataSetSinkApp {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment


//    new CsvTableSink()

    writeAsTextFunc(env)

    env.execute("DataSetSinkApp")

  }

  /**
    * writeAsText sink写出
    *
    * 注意
    *   0）可以写到所有已知的文件系统
    *   1）sink 写出必须 执行env.execute
    *   2）并行度为大于1时才会生成文件夹
    *
    * @param env
    */
  def writeAsTextFunc(env: ExecutionEnvironment): Unit = {
    val ds = env.fromCollection(1 to 10)
    ds.writeAsText("data/output/text/test.text",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(2)
  }
}
