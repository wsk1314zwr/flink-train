package com.wsk.bigdata

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

/**
  * 分布式累加器(计数器)
  */
object CountApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("hadoop", "spark", "flink", "strom", "pySpark")

    //当并行度大于1时 该方式无法进行分布式累加计数操作
    //    data.map(new RichMapFunction[String,Int] {
    //      var count = 0
    //      override def map(value: String): Int = {
    //        count += 1
    //        println("count:"+count)
    //        count
    //      }
    //    }).setParallelism(2).print()

    val info: DataSet[String] = data.map(new RichMapFunction[String, String] {
      //创建计数器
      private val count = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        //注册 计数器
        getRuntimeContext.addAccumulator("long-count", count)
      }

      override def map(value: String): String = {
        count.add(1)
        println("count:" + count)
        value
      }
    })
    val path = "data/output/text/test.text"
    info.writeAsText(path, FileSystem.WriteMode.OVERWRITE).setParallelism(3)
//    info.print()
    val jobResult = env.execute("CountApp")
    val num = jobResult.getAccumulatorResult[Long]("long-count")
    println(num)

  }

}
