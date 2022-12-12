package com.wsk.flink15.java.rocketmq;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.flink.legacy.RocketMQConfig;
import org.apache.rocketmq.flink.legacy.RocketMQSink;
import org.apache.rocketmq.flink.legacy.RocketMQSourceFunction;
import org.apache.rocketmq.flink.legacy.common.config.OffsetResetStrategy;
import org.apache.rocketmq.flink.legacy.common.serialization.SimpleKeyValueDeserializationSchema;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(
                RocketMQConfig.NAME_SERVER_ADDR, "10.199.150.168:9876;10.199.150.169:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c002");
        consumerProps.setProperty(
                RocketMQConfig.CONSUMER_TOPIC, "dev%datark-flink-simple-ddl-source");

        Properties producerProps = new Properties();
        producerProps.setProperty(
                RocketMQConfig.NAME_SERVER_ADDR, "10.199.150.168:9876;10.199.150.169:9876");

        RocketMQSourceFunction<Map> source =
                new RocketMQSourceFunction<>(
                        new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps);
        // use group offsets.
        // If there is no committed offset,consumer would start from the latest offset.
        source.setStartFromGroupOffsets(OffsetResetStrategy.LATEST);
        env.addSource(source)
                .name("rocketmq-source")
                .setParallelism(1)
                .process(
                        new ProcessFunction<Map, Map>() {
                            @Override
                            public void processElement(Map in, Context ctx, Collector<Map> out) {
                                HashMap result = new HashMap<>();
                                result.put("id", in.get("id"));
                                String[] arr = in.get("address").toString().split("\\s+");
                                result.put("province", arr[arr.length - 1]);
                                out.collect(result);
                            }
                        })
                .name("upper-processor")
                .setParallelism(1)
                .process(
                        new ProcessFunction<Map, Message>() {
                            @Override
                            public void processElement(
                                    Map value, Context ctx, Collector<Message> out) {
                                String jsonString = JSONObject.toJSONString(value);
                                Message message =
                                        new Message(
                                                "dev%datark-flink-simple-ddl-sink",
                                                "wsk-test",
                                                jsonString.getBytes(StandardCharsets.UTF_8));
                                out.collect(message);
                            }
                        })
                .addSink(new RocketMQSink(producerProps).withBatchFlushOnCheckpoint(true))
                .name("rocketmq-sink")
                .setParallelism(1);

        try {
            env.execute("rocketmq-flink-example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
