package com.wsk.bigdata.transformations

import java.{lang, util}

import com.wsk.bigdata.source.custome.CustomeRichParallelSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * Data Streeam Transformations 操作,很多算子和DataSet一致
  */
object DataStreamTransformationApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
//    filterFunction(env)
//    unionFunction(env)

    splitFunction(env)
    env.execute("DataStreamTransformationApp")

  }

  /**
    * split & select 流
    * @param env
    */
  def splitFunction(env:StreamExecutionEnvironment): Unit ={
    val data1 = env.addSource(new CustomeRichParallelSource()).setParallelism(1)
    val splits: SplitStream[Long] = data1.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }

        list
      }
    })
    splits.select("even","odd").print()

  }


  /**
    * map & filter 算子
    * @param env
    */
  def filterFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomeRichParallelSource())
    data.map(x => x).filter(_ % 2 == 0).print().setParallelism(1)

  }


  /**
    * union 联合
    * @param env
    */
  def unionFunction(env:StreamExecutionEnvironment): Unit ={
    val data1 = env.addSource(new CustomeRichParallelSource()).setParallelism(1)
    val data2 = env.addSource(new CustomeRichParallelSource()).setParallelism(1)
    data1.union(data2).print().setParallelism(1)
  }


}
