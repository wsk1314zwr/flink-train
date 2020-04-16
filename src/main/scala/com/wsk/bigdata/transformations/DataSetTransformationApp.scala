package com.wsk.bigdata.transformations

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer


/**
  * DataSet 的transformations 算子，其和Spark非常的相似
  */
object DataSetTransformationApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    //    mapFunctin(env)

    //    filterFunction(env)

    //    mapPartitionFunction(env)

    //    firtNFunction(env)

//    flatMapFunction(env)

    distinctFunction(env)

    //    innerJoinFunction(env)

    //    outerJoinFunction(env)

    //    crossFunction(env)
  }


  /**
    * 去重
    * @param env
    */
  def distinctFunction(env:ExecutionEnvironment): Unit ={
    val texts = ListBuffer[String]()
    texts.append("hadoop,flink")
    texts.append("hadoop,sqoop")
    texts.append("hadoop,flink")
    texts.append("flink,flink")
    val ds = env.fromCollection(texts)

    ds.flatMap(_.split(","))
      .distinct()
      .print()
  }

  /**
    * 平铺
    *
    * @param env
    */
  def flatMapFunction(env: ExecutionEnvironment): Unit = {
    val texts = ListBuffer[String]()
    texts.append("hadoop,flink")
    texts.append("hadoop,sqoop")
    texts.append("hadoop,flink")
    texts.append("flink,flink")

    val ds = env.fromCollection(texts)
    ds.flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()

  }

  /**
    * 取First N，可进行分组取TopN
    *
    * @param env
    */
  def firtNFunction(env: ExecutionEnvironment): Unit = {
    val data: ListBuffer[(Int, String)] = ListBuffer[(Int, String)]()
    data.append((1, "hadoop"))
    data.append((1, "spark"))
    data.append((1, "flink"))
    data.append((2, "sqoop"))
    data.append((2, "datax"))
    data.append((2, "maxwell"))
    data.append((3, "hive"))
    data.append((3, "hbase"))
    data.append((3, "impala"))
    data.append((3, "hdfs"))
    val dataSet = env.fromCollection(data)
    dataSet.first(2).print()
    println("=======分割线==========")
    dataSet.groupBy(_._1).first(2).print()
    println("=======分割线==========")
    dataSet.groupBy(_._1).sortGroup(_._2, Order.ASCENDING).first(2).print()
  }

  /**
    * mapPartition,在数据写出外部时 非常重要。减少连接数
    *
    * @param env
    */
  def mapPartitionFunction(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(1 to 100).setParallelism(5)
    data.mapPartition(partition => {
      println("进入分区")
      partition.map(x => {
        println(x)

      })
    }).print()


  }

  /**
    * filter
    *
    * @param env
    */
  def filterFunction(env: ExecutionEnvironment) {
    val data = env.fromCollection(1 to 10)
    data.filter(_ >= 5).print()
  }

  /**
    * map
    *
    * @param env
    */
  def mapFunctin(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(1 to 10)
    data.map(_ + 1).print()
  }


  /**
    * innerjoin 操作
    *
    * @param env
    */
  def innerJoinFunction(env: ExecutionEnvironment): Unit = {

    val data1 = ListBuffer[(Int, String)]() // id name
    data1.append((1, "zwr"))
    data1.append((2, "wsk"))
    data1.append((3, "小wsk"))

    val data2 = ListBuffer[(Int, String)]() //id adress
    data2.append((1, "北京"))
    data2.append((2, "上海"))
    data2.append((3, "上海"))

    val left = env.fromCollection(data1)
    val right = env.fromCollection(data2)

    left.join(right)
      .where(0)
      .equalTo(0)
      .apply((left, right) => {
        (left._1, left._2, right._2)
      })
      .print()

  }

  /**
    * outerJoin 操作
    *
    * @param env
    */
  def outerJoinFunction(env: ExecutionEnvironment): Unit = {
    val data1 = ListBuffer[(Int, String)]() // id name
    data1.append((1, "zwr"))
    data1.append((2, "wsk"))
    data1.append((3, "小wsk"))

    val data2 = ListBuffer[(Int, String)]() //id 户籍
    data2.append((1, "北京"))
    data2.append((2, "上海"))
    data2.append((4, "上海"))

    val left = env.fromCollection(data1)
    val right = env.fromCollection(data2)
    println("======= 左外连接 =============")
    //左外连接
    left.leftOuterJoin(right)
      .where(0)
      .equalTo(0)
      .apply((left, right) => {
        if (right == null) {
          (left._1, left._2, "-")
        } else {
          (left._1, left._2, right._2)
        }
      })
      .print()

    println("======= 右外连接 =============")
    //右外连接
    left.rightOuterJoin(right)
      .where(0)
      .equalTo(0)
      .apply((left, right) => {
        if (left == null) {
          (right._1, "-", right._2)
        } else {
          (right._1, left._2, right._2)
        }
      })
      .print()
    println("======= 全外连接 =============")
    //全外连接
    left.fullOuterJoin(right)
      .where(0)
      .equalTo(0)
      .apply((left, right) => {
        if (left == null) {
          (right._1, "-", right._2)
        } else if (right == null) {
          (left._1, left._2, "-")
        } else {
          (right._1, left._2, right._2)
        }
      })
      .print()
  }

  /**
    * cross 操作，笛卡尔积操作
    *
    * @param env
    */
  def crossFunction(env: ExecutionEnvironment): Unit = {
    val data1 = List("wsk", "zwr")
    val data2 = List("001", "002")

    val left = env.fromCollection(data1)
    val right = env.fromCollection(data2)
    left.cross(right).print()
  }


}
