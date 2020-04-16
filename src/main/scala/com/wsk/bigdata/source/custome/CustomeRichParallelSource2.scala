package com.wsk.bigdata.source.custome

import java.util.Date

import com.wsk.bigdata.table.StreamTableSQLApp.SalesLog
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class CustomeRichParallelSource2 extends RichParallelSourceFunction[SalesLog] {
  var count = 0L
  var isRuning = true


  override def cancel(): Unit = {
    isRuning = false
  }

  override def run(ctx: SourceFunction.SourceContext[SalesLog]): Unit = {

    while (isRuning) {
//      Thread.sleep(1000)
      ctx.collect(SalesLog("1", ""+new Random().nextInt(5), "1", 10.1,new Date().getTime))
//      ctx.collect(SalesLog("1", "1", "1", 10.1,new Date().getTime))

    }
  }

}
