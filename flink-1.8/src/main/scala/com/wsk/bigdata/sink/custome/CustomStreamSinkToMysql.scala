package com.wsk.bigdata.sink.custome

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class CustomStreamSinkToMysql extends RichSinkFunction[Student] {
  var connection: Connection = _
  var pstmt: PreparedStatement = _

  def getConnect(): Connection = {
    var conn: Connection = null;
    Class.forName("com.mysql.jdbc.Driver")
    val url: String = "jdbc:mysql://10.199.140.143:3306/wsk_test"
    conn = DriverManager.getConnection(url, "root", "servyou")
    conn
  }


  /**
    *每条记录会执行一次
    * @param value
    * @param context
    */
  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    println("~~~~invoke~~~~~")

    pstmt.setInt(1, value.id)
    pstmt.setString(2, value.name)
    pstmt.setInt(3, value.age)
    pstmt.executeUpdate()

  }

  /**
    * 生命周期方法，开始时执行
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    connection = getConnect()
    val sql = "INSERT INTO student VALUES(?,?,?)"
    pstmt = connection.prepareStatement(sql)

    println("~~~~open~~~~~")

  }

  /**
    * 生命周期方法，结束时执行
    *
    */
  override def close(): Unit = {
    if (pstmt != null) {
      pstmt.close()
    }

    if (connection != null) {
      connection.close()
    }
  }
}
