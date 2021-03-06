package com.wsk.bigdata.source.custome

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * 通过SourceFunction生成的 Source是不能并行运行的
  */
class CustomeSource extends SourceFunction[Long] {

  var count = 0L
  var isRuning = true


  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRuning && count < 1000) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRuning = false
  }
}