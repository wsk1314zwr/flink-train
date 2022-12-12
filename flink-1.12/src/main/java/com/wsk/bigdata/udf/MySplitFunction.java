package com.wsk.bigdata.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Description: 将一行按照一定格式拆成多行
 * @Auther: wsk
 * @Date: 2019/12/13 15:05
 * @Version: 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class MySplitFunction extends TableFunction<Row> {

    public void eval(String str,String splitStr) {
        try {
            String[] split = str.split(splitStr);
            for (String s : split) {
                collect(Row.of(s, s.length()));
            }
        }catch (Exception e){
            collect(null);
        }

    }
}
