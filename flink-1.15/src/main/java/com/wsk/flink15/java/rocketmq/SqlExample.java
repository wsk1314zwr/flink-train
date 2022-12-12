package com.wsk.flink15.java.rocketmq;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class SqlExample {
    public static void main(String[] args) {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        // .inBatchMode()
                        .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Create a source table (using SQL DDL)
        tableEnv.executeSql(
                "CREATE TABLE rocketmq_source (\n"
                        + "  `id` BIGINT,\n"
                        + "  `name` BIGINT"
                        + ") WITH (\n"
                        + "  'connector' = 'rocketmq',\n"
                        + "  'topic' = 'dev%datark-flink-simple-ddl-source',\n"
                        + "  'consumerGroup' = 'c002',\n"
                        + "  'nameServerAddress' = '10.199.150.168:9876;10.199.150.169:9876'\n"
                        + ")");

        // Create a sink table (using SQL DDL)
        tableEnv.executeSql(
                "CREATE TABLE rocketmq_sink (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `item_id` BIGINT"
                        + ") WITH (\n"
                        + "  'connector' = 'rocketmq',\n"
                        + "  'producerGroup' = 'c002_producer',\n"
                        + "  'topic' = 'dev%datark-flink-simple-ddl-sink',\n"
                        + "  'nameServerAddress' = '10.199.150.168:9876;10.199.150.169:9876'\n"
                        + ")");

        // Create a source table (using SQL DDL)
        tableEnv.executeSql("insert into  rocketmq_sink select * from rocketmq_source");
    }
}
