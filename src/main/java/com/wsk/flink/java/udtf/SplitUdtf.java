package com.wsk.flink.java.udtf;

import org.apache.flink.table.functions.TableFunction;

/**
 * @Description: 将一行按照一定格式拆成多行
 * @Auther: wsk
 * @Date: 2019/12/13 15:05
 * @Version: 1.0
 */
public class SplitUdtf extends TableFunction<String> {

    public void eval(String str,String splitStr) {
        try {
            String[] split = str.split(splitStr);
            for (String s : split) {
                collect(s);
            }
        }catch (Exception e){
            collect(null);
        }

    }
}
