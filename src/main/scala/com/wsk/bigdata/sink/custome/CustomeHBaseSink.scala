package com.wsk.bigdata.sink.custome

import com.wsk.bigdata.streaming.SockerToHBaseStreamApp.Wc
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Increment, Table}
import org.apache.hadoop.hbase.util.Bytes

class CustomeHBaseSink(tableName: String, family: String) extends RichSinkFunction[Wc] {


  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "vnm1:2181,vnm2:2181")
    conn = ConnectionFactory.createConnection(conf)
  }

  override def invoke(wc: Wc, context: SinkFunction.Context[_]): Unit = {

    val t: Table = conn.getTable(TableName.valueOf(tableName))

    //    val put: Put = new Put(Bytes.toBytes(wc.word))
    //    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("target"), Bytes.toBytes(wc.count))
    val increment = new Increment(Bytes.toBytes(wc.word))
    increment.addColumn(Bytes.toBytes(family), Bytes.toBytes("target"), wc.count)

    t.increment(increment)
    t.close()
  }

  override def close(): Unit = {
    super.close()
    conn.close()
  }
}