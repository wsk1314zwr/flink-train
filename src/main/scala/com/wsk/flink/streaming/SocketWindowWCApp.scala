package com.wsk.flink.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * socket 窗口单词统计
  *
  *
  */
object SocketWindowWCApp {

  var hostname : String = _
  var port: Int = 9099


  def main(args: Array[String]): Unit = {

    try {
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "10.199.140.143"
      port = params.getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
          "is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l <port>' " +
          "and type the input text into the command line")
        //        return
      }

    }

    //step1:获取 运行的ENV 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //step2: source 获取DataStream
//    import org.apache.flink.api.scala._
    val lines = env.socketTextStream(hostname, port)

    //step3:transformations 转换
    val windowCounts = lines
      .flatMap { w => w.split(",") }
      .map { w => WordCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(4), Time.seconds(2))
      .sum("count")

    //step4:sink
    windowCounts.print().setParallelism(1)

    //step5: exe
    env.execute("Socket Window WordCount")
  }

  case class WordCount(word: String, count: Long)

}
