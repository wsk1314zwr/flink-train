package com.wsk.flink13.java.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * @description:
 * @author: wsk
 * @date: 2021/6/29 11:27
 * @version: 1.0
 */
public class SQLTest {
    public static void main(String[] args) {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        TableResult tableResult = tableEnv.executeSql("SELECT order_id, price FROM (VALUES (1, 2.0), (2, 3.1))  AS t (order_id, price)");
        tableResult.print();

        Table table1 = tableEnv.sqlQuery("SELECT order_id, name, price FROM (VALUES (1, 'bob',2.0), (2, 'bob', 3.1))  AS t (order_id,name, price)");
        TableResult tableResult2 = tableEnv.executeSql("SELECT name, sum(price) from " + table1 + " group by name");
        tableResult2.print();
    }
}
