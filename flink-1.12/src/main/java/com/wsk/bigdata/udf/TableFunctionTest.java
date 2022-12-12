package com.wsk.bigdata.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableFunctionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporarySystemFunction("wsksplit", MySplitFunction.class);
        Table table1 = tableEnv.sqlQuery("SELECT order_id, name, price FROM (VALUES (1, 'bHbb',2.0), (2, 'aaHa', 3.1))  AS t (order_id,name, price)");
        TableResult tableResult2 = tableEnv.executeSql("SELECT order_id, name, price, word, length  from " + table1 +
                " LEFT JOIN LATERAL TABLE(wsksplit(name, 'H')) ON TRUE");
        tableResult2.print();
    }
}
