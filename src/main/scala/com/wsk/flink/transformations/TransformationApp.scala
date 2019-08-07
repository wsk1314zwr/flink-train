package com.wsk.flink.transformations

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import scala.collection.mutable.ListBuffer


object TransformationApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

//    innerJoinFunction(env)
//    outerJoinFunction(env)
    crossFunction(env)
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
        if(right == null){
          (left._1, left._2, "-")
        }else{
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
        if(left == null){
          (right._1, "-", right._2)
        }else{
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
        if(left == null){
          (right._1, "-", right._2)
        }else if(right == null){
          (left._1, left._2, "-")
        }else{
          (right._1, left._2, right._2)
        }
      })
      .print()
  }

  /**
    * cross 操作，笛卡尔积操作
    * @param env
    */
  def crossFunction(env: ExecutionEnvironment): Unit ={
    val data1 = List("wsk","zwr")
    val data2 = List("001","002")

    val left = env.fromCollection(data1)
    val right = env.fromCollection(data2)
    left.cross(right).print()
  }



}
