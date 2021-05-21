package com.wsk.bigdata.table

import com.wsk.bigdata.source.BatchSourceApp.Teacher
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row

object BatchTableSQLAPP {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // step1: 创建 table env
    val tEnv = BatchTableEnvironment.create(env)

    //    val saleDs: DataStream[SalesLog] = env.fromCollection(generationData())
    val dataSet = env.readCsvFile[Teacher](" data/input/source/csv/test.csv",
      ignoreFirstLine = true,
      pojoFields = Array("name", "age"))
    val table = tEnv.fromDataSet(dataSet, 'name, 'age)

    val result: Table = tEnv.sqlQuery(s"select * from ${table}")


    val csvTableSink = new CsvTableSink("data/output/table3.csvsink")
    val fieldNames: Array[String] = Array("name","age")
    tEnv.registerTableSink("CsvSinkTable", fieldNames, Array(Types.STRING,Types.INT), csvTableSink)

    result.insertInto("CsvSinkTable")

    env.execute("运行！")
  }
}
