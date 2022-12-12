package com.wsk.bigdata.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 自定义一个substring scalar function
 */
public class MySubStringFunction extends ScalarFunction {
    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, end);
    }
}
