package com.wsk.bigdata.cdnproject

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
  */
class PKMySQLSource extends RichParallelSourceFunction[mutable.HashMap[String, String]] {

  var isRun = true
  var connection: Connection = _
  var ps: PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    val dirver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://10.199.140.143/wsk_test?characterEncoding=utf-8"
    val user = "root"
    val passwd = "servyou"
    Class.forName(dirver)
    connection = DriverManager.getConnection(url, user, passwd)
    var sql = "select domain,user_id from user_domain_config "
    ps = connection.prepareStatement(sql)
    println("初始化，创建connection连接对象")
  }

  override def close(): Unit = {
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  override def cancel(): Unit = {
    isRun = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {

    while (isRun) {
      val rs = ps.executeQuery()
      val map = new mutable.HashMap[String, String]()
      while (rs.next()) {
        map.put(rs.getString(1), rs.getString(2))
      }
      println("获取Mysql中的配置信息！")
      sourceContext.collect(map)
      TimeUnit.SECONDS.sleep(5)
    }
  }
}
