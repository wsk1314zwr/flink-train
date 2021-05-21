//package com.wsk.bigdata.streaming.window
//
//import org.apache.flink.types.Row
//import org.junit.Assert.assertEquals
//import scala.collection.mutable
//
///**
// * @Description:
// * @Auther: wsk
// * @Date: 2020/5/8 10:55
// * @Version: 1.0
// */
//object WindowJoinITCase {
//    def main(args: Array[String]): Unit = {
//        val sqlQuery =
//            """
//              |SELECT t1.key, t2.id, t1.id
//              |FROM T1 AS t1 LEFT OUTER JOIN  T2 AS t2 ON
//              | t1.key = t2.key AND
//              | t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
//              | t2.rowtime + INTERVAL '6' SECOND AND
//              | t1.id <> 'L-5'
//      """.stripMargin
//
//        val data1 = new mutable.MutableList[(String, String, Long)]
//        // for boundary test
//        data1.+=(("A", "L-1", 1000L))
//        data1.+=(("A", "L-2", 2000L))
//        data1.+=(("B", "L-4", 4000L))
//        data1.+=(("B", "L-5", 5000L))
//        data1.+=(("A", "L-6", 6000L))
//        data1.+=(("C", "L-7", 7000L))
//        data1.+=(("A", "L-10", 10000L))
//        data1.+=(("A", "L-12", 12000L))
//        data1.+=(("A", "L-20", 20000L))
//
//        val data2 = new mutable.MutableList[(String, String, Long)]
//        data2.+=(("A", "R-6", 6000L))
//        data2.+=(("B", "R-7", 7000L))
//        data2.+=(("D", "R-8", 8000L))
//        data2.+=(("A", "R-11", 11000L))
//
//        val t1 = env.fromCollection(data1)
//                .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
//                .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
//
//        val t2 = env.fromCollection(data2)
//                .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
//                .toTable(tEnv, 'key, 'id, 'rowtime.rowtime)
//
//        tEnv.registerTable("T1", t1)
//        tEnv.registerTable("T2", t2)
//
//        val sink = new TestingAppendSink
//        val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
//        result.addSink(sink)
//        println(sink.getAppendResults)
//        env.execute()
//        val expected = mutable.MutableList[String](
//            "A,R-6,L-1",
//            "A,R-6,L-2",
//            "A,R-6,L-6",
//            "A,R-6,L-10",
//            "A,R-6,L-12",
//            "B,R-7,L-4",
//            "A,R-11,L-6",
//            "A,R-11,L-10",
//            "A,R-11,L-12",
//            "B,null,L-5",
//            "C,null,L-7",
//            "A,null,L-20")
//
//        assertEquals(expected.toList.sorted, sink.getAppendResults.sorted)
//
//    }
//
//}
