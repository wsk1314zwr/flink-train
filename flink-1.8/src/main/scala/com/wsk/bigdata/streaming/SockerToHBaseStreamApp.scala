package com.wsk.bigdata.streaming

import java.sql.Timestamp

import com.wsk.bigdata.sink.custome.CustomeHBaseSink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
  */

object SockerToHBaseStreamApp {


  private val extractor = new AscendingTimestampExtractor[Tuple2[String, Long]]() {
    override def extractAscendingTimestamp(element: Tuple2[String, Long]): Long = element._2
  }

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    val stream = env.socketTextStream("10.199.140.144", 9099)

    val markstream = stream.map(x => {
      val fields = x.split(",")
      (fields(0), fields(1).toLong)
    }).setParallelism(1).assignTimestampsAndWatermarks(extractor)

    tEnv.registerDataStream("words", markstream, 'word, 'fronttime.rowtime)

    val resultTable = tEnv.sqlQuery("SELECT word,count(word),TUMBLE_ROWTIME(fronttime, INTERVAL '5' SECOND)," +
      " TUMBLE_START(fronttime, INTERVAL '5' SECOND),TUMBLE_END(fronttime, INTERVAL '5' SECOND) from  words GROUP BY TUMBLE(fronttime, INTERVAL '5' SECOND),word")
    //    println(tEnv.explain(maxIpStream1min))

    val appendStream = resultTable.toAppendStream[Wc]
    appendStream.print()

    appendStream.addSink(new CustomeHBaseSink("wsk_test", "info"))
    env.execute("SockerToHBaseStreamApp")

  }

  case class Wc(word: String, count: Long, time1: Timestamp, time2: Timestamp, time3: Timestamp)

}