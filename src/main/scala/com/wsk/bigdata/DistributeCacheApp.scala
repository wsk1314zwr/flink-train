package com.wsk.bigdata

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * 分布式缓存，Flink程序的每个Worker节点可并行的从外部缓存文件中读取数据
  */
object DistributeCacheApp {

  def main(args: Array[String]): Unit = {
    val evn = ExecutionEnvironment.getExecutionEnvironment

    //step1:注册一个本地的文件
    evn.registerCachedFile("data/cache/1","localFile")
    val dataSet = evn.fromElements("hadoop","spark","flink","pyspark").setParallelism(2)

    dataSet.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit ={
        //step2：在opean方法中读取缓存中的数据
        val localFile = getRuntimeContext.getDistributedCache.getFile("localFile")
        val records = FileUtils.readLines(localFile)//java
        import scala.collection.JavaConversions._
        for(record <- records){//scala
          println("读取缓存中的数据："+record)
        }

      }

      override def map(value: String): String = {


        value
      }
    }).print()
  }
}
