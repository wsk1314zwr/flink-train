package com.wsk.bigdata.source

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
  * Batch source 编程
  */
object BatchSourceApp {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    fromCollection(env)

    //    fromCsv(env)

    //    fromRecursive(env)

//    fromComCompressFile(env)

  }

  /**
    *从 集合中生成 DataStream
    * @param env
    */
  def fromCollection(env: ExecutionEnvironment): Unit ={
     val value = env.fromCollection(1 to 100)
    value.map(_+1).print()
  }

  /**
    * 从 CSv中获取 DataSet
    *
    * @param env
    */
  def fromCsv(env: ExecutionEnvironment): Unit = {

    val dataSet = env.readCsvFile[Teacher](" data/input/source/csv/test.csv",
      ignoreFirstLine = true,
      pojoFields = Array("name", "age"))
    dataSet.print()
  }

  /**
    * 从递归的文件夹中读取数据
    *
    * @param env
    */
  def fromRecursive(env: ExecutionEnvironment): Unit = {
    val conf = new Configuration()
    conf.setBoolean("recursive.file.enumeration", true)
    val dataSet = env.readCsvFile[Teacher]("data/input/source/recursive",
      ignoreFirstLine = true,
      pojoFields = Array("name", "age")).withParameters(conf)
    dataSet.print()


  }

  /**
    * 读取压缩文件
    *
    * @param env
    */
  def fromComCompressFile(env: ExecutionEnvironment): Unit = {
    val dataSet = env.readTextFile("data/inpusource/CompressFilet/")
    dataSet.print()

  }

  case class Teacher(name: String, age: Int)

}
