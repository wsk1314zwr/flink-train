package com.wsk.flink13.java.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description:
 * @author: wsk
 * @date: 2021/6/29 16:54
 * @version: 1.0
 */
public class UnionTest {
    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql("set 'table.planner' = 'blink' ");
        tableEnv.executeSql("create view t1(s) as values ('c'), ('a'), ('b'), ('b'), ('c')");
        tableEnv.executeSql("create view t2(s) as values ('d'), ('e'), ('a'), ('b'), ('b')");
        TableResult result = tableEnv.executeSql("(SELECT s FROM t1) UNION (SELECT s FROM t2)");
        TableResult result2 = tableEnv.executeSql("(SELECT s FROM t1) UNION ALL(SELECT s FROM t2)");
        result.print();
        result2.print();

    }
}
