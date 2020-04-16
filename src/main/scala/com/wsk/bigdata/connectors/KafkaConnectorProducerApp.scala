package com.wsk.bigdata.connectors

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
  * Kafka Connector sink 操作
  */
object KafkaConnectorProducerApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint常用设置参数
//        env.enableCheckpointing(4000)
    //        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //        env.getCheckpointConfig.setCheckpointTimeout(10000)
    //        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 从socket接收数据，通过Flink，将数据Sink到Kafka
    val data = env.socketTextStream("10.199.140.144", 9099)

    val topic = "wsk_test_2019"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.199.151.15:9092")

//    val kafkaSink = new FlinkKafkaProducer[String](topic,
//      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
//      properties)

    val kafkaSink = new FlinkKafkaProducer[String](topic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      properties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    data.addSink(kafkaSink)

    env.execute("KafkaConnectorProducerApp")
  }
}
