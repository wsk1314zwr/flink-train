package com.wsk.flink15.java.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

/**
 * @description: Table Api 计算列测试
 * @author: wsk
 * @date: 2021/6/25 17:39
 * @version: 1.0
 */
public class TableAPIExpressColumnTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<User> dataStream =
                env.fromElements(
                        new User("Alice", 4, Instant.ofEpochMilli(1000)),
                        new User("Bob", 6, Instant.ofEpochMilli(1001)),
                        new User("Alice", 10, Instant.ofEpochMilli(1002)));


        // === EXAMPLE 1 ===

        // derive all physical columns automatically

        Table table = tableEnv.fromDataStream(dataStream);
        table.printSchema();
        // prints:
        // (
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9)
        // )

        // === EXAMPLE 2 ===

        // derive all physical columns automatically
        // but add computed columns (in this case for creating a proctime attribute column)

        Table table2 = tableEnv.fromDataStream(
                dataStream,
                Schema.newBuilder()
                        .columnByExpression("proc_time", "PROCTIME()")
                        .build());
        table2.printSchema();
        // prints:
        // (
        //  `name` STRING,
        //  `score` INT NOT NULL,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
        //)

        // === EXAMPLE 3 ===

        // derive all physical columns automatically
        // but add computed columns (in this case for creating a rowtime attribute column)
        // and a custom watermark strategy
        Table table3 =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                                .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                                .build());
        table3.printSchema();
        // prints:
        // (
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
        //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
        // )

        // === EXAMPLE 4 ===

        // derive all physical columns automatically
        // but access the stream record's timestamp for creating a rowtime attribute column
        // also rely on the watermarks generated in the DataStream API

        // we assume that a watermark strategy has been defined for `dataStream` before
        // (not part of this example)
        Table table4 =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                                .watermark("rowtime", "SOURCE_WATERMARK()")
                                .build());
        table4.printSchema();
        // prints:
        // (
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
        //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
        // )

        // === EXAMPLE 5 ===

        // define physical columns manually
        // in this example,
        //   - we can reduce the default precision of timestamps from 9 to 3
        //   - we also project the columns and put `event_time` to the beginning

        Table table5 =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("event_time", "TIMESTAMP_LTZ(3)")
                                .column("name", "STRING")
                                .column("score", "INT")
                                .watermark("event_time", "SOURCE_WATERMARK()")
                                .build());
        table5.printSchema();
        // prints:
        // (
        //  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
        //  `name` VARCHAR(200),
        //  `score` INT
        // )
        // note: the watermark strategy is not shown due to the inserted column reordering projection

    }
}


