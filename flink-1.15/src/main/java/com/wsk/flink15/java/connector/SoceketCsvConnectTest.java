package com.wsk.flink15.java.connector;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用自定义的 SocketDynamicTableSource 和 ChangelogCsvFormat 进行SQL分析
 */
public class SoceketCsvConnectTest {
    private static final Logger logger = LoggerFactory.getLogger(SoceketCsvConnectTest.class);
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String sourceSql = "CREATE TABLE UserScores (name STRING, score INT)\n" +
                "WITH (\n" +
                "  'connector' = 'socket',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '9999',\n" +
                "  'byte-delimiter' = '10',\n" +
                "  'format' = 'changelog-csv',\n" +
                "  'changelog-csv.column-delimiter' = '|'\n" +
                ")";
        logger.info("regist sql:[{}]", sourceSql);
        tEnv.executeSql(sourceSql);
        TableResult tableResult = tEnv.executeSql("SELECT name, SUM(score) FROM UserScores GROUP BY name");
        tableResult.print();
    }
}
