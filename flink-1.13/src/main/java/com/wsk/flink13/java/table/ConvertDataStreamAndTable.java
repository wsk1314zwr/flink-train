package com.wsk.flink13.java.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @description: DataStream就是老版本的AppendStream ，toAppendStream()在以后的版本将会删除
 * @author: wsk
 * @date: 2021/6/25 16:44
 * @version: 1.0
 */
public class ConvertDataStreamAndTable {
    public static void main(String[] args) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream);

        // register the Table object as a view and query it
        tableEnv.createTemporaryView("InputTable", inputTable);
        Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");

        // interpret the insert-only Table as a DataStream again
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        // add a printing sink and execute in DataStream API
        resultStream.print();

        //转成RetractStream
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultTable, Row.class);
        tuple2DataStream.print();

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);
        rowDataStream.print();

        env.execute();
    }
}
