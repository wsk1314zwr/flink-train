package com.wsk.bigdata.batch


import com.wsk.flink.java.udtf.SplitUdtf
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._


/**
  * @Description: 一行拆为多行测试
  * @Auther: wsk
  * @Date: 2019/12/13 15:08
  * @Version: 1.0
  */
object SplitUdtfTest {

    def main(args: Array[String]): Unit = {
        //得到批环境
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val dataSet = env.fromElements((1, "小明", 15, "男", 1500), (1, "小明", 15, "女", 1500), (1, "小明", 15, "变性", 1500), (1, "小明", 15, "同性",
                1500), (1, "小明", 15, "未知", 1500))
//        val dataSetGrade = env.fromElements((1,"语文<||>数学",100))

        //得到Table环境
        val tableEnv = TableEnvironment.getTableEnvironment(env)
        tableEnv.registerFunction("splitUdtf", new SplitUdtf())
        //注册table
        tableEnv.registerDataSet("user", dataSet, 'id, 'name, 'age, 'sex, 'salary)
//        tableEnv.registerDataSet("grade",dataSetGrade,'userId,'name,'fraction)


        //内连接，两个表
        // tableEnv.sqlQuery("select * FROM `user`  INNER JOIN  grade on  `user`.id = grade.userId ")
//        tableEnv.sqlQuery("select `user`.*,grade.name,grade.fraction FROM `user`  INNER JOIN  grade on  " +
//                "`user`.id = grade.userId ")
        tableEnv.sqlQuery("select '' as a ,`user`.id ,cast('2011-05-09' as datetime) as t  from `user` group by " +
                "`user`.id ")
                .first(10).print()
//        env.execute()

        //lsit agg函数测试
//        val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//        val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//        val tableEnv = StreamTableEnvironment.create(environment, settings)
//
//        //得到批环境
//        val env = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setParallelism(1)
//
//        val dataStream: DataStream[(Int, String, Int, String, Int)] = env.fromElements((1, "小明", 15, "男", 1500), (1,
//                "小明", 15,
//                "女", 1500), (1, "小明", 15, "变性", 1500), (1, "小明", 15, "同性",
//                1500), (1, "小明", 15, "未知", 1500))
//
//
//        //注册table
//        tableEnv.registerDataStream("user", dataStream, 'id, 'name, 'age, 'sex, 'salary)
//
//
//        val queryTable = tableEnv.sqlQuery("select `user`.id,LISTAGG(`user`.sex,';') from `user` group by `user`.id ")
//        tableEnv.toRetractStream[Row](queryTable).print()
//
//        environment.execute()


    }
}
