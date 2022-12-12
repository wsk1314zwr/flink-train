package com.wsk.flink13.java.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * @description:
 * @author: wsk
 * @date: 2021/6/28 14:55
 * @version: 1.0
 */
public class ConvertChangeLogStreamAndTable2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

        // interpret the DataStream as a Table
        Table table = tableEnv.fromChangelogStream(dataStream);

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable", table);
        tableEnv
                .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
                .print();

        // prints:
        // +----+--------------------------------+-------------+
        // | op |                           name |       score |
        // +----+--------------------------------+-------------+
        // | +I |                            Bob |           5 |
        // | +I |                          Alice |          12 |
        // | -D |                          Alice |          12 |
        // | +I |                          Alice |         100 |
        // +----+--------------------------------+-------------+
        // === EXAMPLE 2 ===

        // interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)

        // create a changelog DataStream
        DataStream<Row> dataStream2 =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

        // interpret the DataStream as a Table
        Table table2 =
                tableEnv.fromChangelogStream(
                        dataStream2,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert());

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable2", table2);
        tableEnv
                .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable2 GROUP BY f0")
                .print();

        // prints:
        // +----+--------------------------------+-------------+
        // | op |                           name |       score |
        // +----+--------------------------------+-------------+
        // | +I |                            Bob |           5 |
        // | +I |                          Alice |          12 |
        // | -D |                          Alice |          12 |
        // | +I |                          Alice |         100 |
        // +----+--------------------------------+-------------+
    }
}

