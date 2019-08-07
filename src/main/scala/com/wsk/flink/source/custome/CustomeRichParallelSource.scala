package com.wsk.flink.source.custome

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class CustomeRichParallelSource extends RichParallelSourceFunction[Long]{
  var count = 0L
  var isRuning = true


  override def cancel(): Unit = {
    isRuning = false
  }

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRuning && count < 1000) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }
}
