package com.wsk.bigdata.table

import java.util.Date

import com.wsk.bigdata.source.custome.CustomeRichParallelSource2
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer

/**
  * Table and SQL API 编程
  *
  * 我这里演示的是 Stream 的Table and SQL API ，
  * 直接写数据到csv文件夹下，注意当输出流为toRetractStream时 SQL中才能有group by等聚合操作，
  * 若是 batch的Table and SQL API的SQL可以的
  */
object StreamTableSQLApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // step1: 创建 table env
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //    val saleDs: DataStream[SalesLog] = env.fromCollection(generationData())

    val saleDs = env.addSource(new CustomeRichParallelSource2())
//    val saleD2 = env.addSource(new CustomeRichParallelSource2())

    //    saleDs.print()
    // step2:将Stream转成Table
    val saleTable: Table = tableEnv.fromDataStream(saleDs)
//    val saleTable2: Table = tableEnv.fromDataStream(saleD2)
    // step3: Table 注册成一张表
    tableEnv.registerTable("sale", saleTable)
//    tableEnv.registerTable("sale2", saleTable2)
    // step4: 查询表
    val resultTable: Table = tableEnv.sqlQuery("select customerID,sum(money)  from sale group by customerID")

//      "t2.customerID ")
//    val resultTable: Table = tableEnv.sqlQuery("select count(1)  from sale  " +
//      "t2.customerID ")
    //step5：将结果写到指定的目录
//        val csvTableSink = new CsvTableSink("data/output/table2.csvsink")
//        val fieldNames: Array[String] = Array("customerID")
//        tableEnv.registerTableSink("CsvSinkTable", fieldNames, Array(Types.STRING), csvTableSink)
////        resultTable.insertInto("CsvSinkTable")
//    tableEnv.sqlUpdate(s"insert into CsvSinkTable select * from $resultTable")
//     step5：将结果转成流 打印出来

        tableEnv.toRetractStream[Row](resultTable).print()

//    tableEnv.toRetractStream[Row](resultTable)
//      .print()
    env.execute("TableSQLApp")

  }

  def generationData(): ListBuffer[SalesLog] = {
    val salesLogs = new ListBuffer[SalesLog]()
    salesLogs.append(SalesLog("1", "1", "1", 10.1, new Date().getTime))
    salesLogs.append(SalesLog("2", "1", "2", 30.1, new Date().getTime))
    salesLogs.append(SalesLog("3", "1", "4", 40.1, new Date().getTime))
    salesLogs.append(SalesLog("4", "2", "1", 10.1, new Date().getTime))
    salesLogs.append(SalesLog("5", "2", "2", 10.1, new Date().getTime))
    salesLogs.append(SalesLog("6", "3", "6", 10.1, new Date().getTime))
    salesLogs.append(SalesLog("7", "3", "6", 10.1, new Date().getTime))
    salesLogs.append(SalesLog("8", "3", "6", 10.1, new Date().getTime))
    salesLogs
  }

  /**
    * 产品销售日志
    *
    * @param transationsID 自增主键ID
    * @param customerID    用户ID
    * @param productId     商品ID
    * @param money         价格
    */
  case class SalesLog(transationsID: String,
                      customerID: String,
                      productId: String,
                      money: Double, createTime: Long)

}
