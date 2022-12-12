package com.wsk.flink13.java.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description: 以 GROUPING SET方式 设置多种group的组合方式，可以使用cube方式代替
 * @author: wsk
 * @date: 2021/6/29 16:14
 * @version: 1.0
 */
public class GroupAggTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

        TableResult table =  tableEnv.executeSql("SELECT supplier_id, rating, COUNT(*) AS total\n" +
                "FROM (VALUES\n" +
                "    ('supplier1', 'product1', 4),\n" +
                "    ('supplier1', 'product2', 3),\n" +
                "    ('supplier2', 'product3', 3),\n" +
                "    ('supplier2', 'product4', 4))\n" +
                "AS Products(supplier_id, product_id, rating)\n" +
                "GROUP BY GROUPING SETS ((supplier_id, rating), (supplier_id),())");
        table.print();
    }
}
