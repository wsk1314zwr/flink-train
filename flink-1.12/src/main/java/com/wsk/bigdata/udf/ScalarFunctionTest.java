package com.wsk.bigdata.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class ScalarFunctionTest {

    static String udfPath = "/Users/skwang/Documents/workspace/workspace4/project/my_project/flink-train/flink-1.12/src/lib/wsk-flink-udf.jar";

    public static void main(String[] args) throws MalformedURLException, InvocationTargetException, IllegalAccessException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 第一种: table api 注册 函数
//        tableEnv.createTemporarySystemFunction("wskSub",MySubStringFunction.class);
        // 第二种: sql 注册函数
//        tableEnv.executeSql("CREATE TEMPORARY SYSTEM  FUNCTION wskSub as 'com.wsk.bigdata.udf.MySubStringFunction'");
        // 第三种: 注销本包的MySubStringFunction类,然后使用 classload加载udf jar ,然后再使用sql加载函数
        ClassLoader currentClassLoade = Thread.currentThread().getContextClassLoader();
        urlClassLoaderAddUrl((URLClassLoader) currentClassLoade, new File(udfPath).toURI().toURL());
        tableEnv.executeSql("CREATE TEMPORARY SYSTEM  FUNCTION wskSub as 'com.wsk.bigdata.udf.MySubStringFunction'");

//        Table table1 = tableEnv.sqlQuery("SELECT order_id, name, price FROM (VALUES (1, 'bob',2.0), (2, 'bob', 3.1))  AS t (order_id,name, price)");
        TableResult tableResult2 = tableEnv.executeSql("SELECT name,wskSub(name, 0,2) as alias, sum(price) from " +
                "(SELECT order_id, name, price FROM (VALUES (1, 'bob',2.0), (2, 'bob', 3.1))  AS t (order_id,name, price)) group by name");
        tableResult2.print();
    }

    private static void urlClassLoaderAddUrl(URLClassLoader classLoader, URL url)
            throws InvocationTargetException, IllegalAccessException {
        Method method = getDeclaredMethod(classLoader, "addURL", URL.class);

        if (method == null) {
            throw new RuntimeException(
                    "can't not find declared method addURL, curr classLoader is "
                            + classLoader.getClass());
        }
        method.setAccessible(true);
        method.invoke(classLoader, url);
    }

    /**
     * get declaredMethod util find
     *
     * @param object
     * @param methodName
     * @param parameterTypes
     * @return
     */
    public static Method getDeclaredMethod(
            Object object, String methodName, Class<?>... parameterTypes) {
        Method method = null;

        for (Class<?> clazz = object.getClass();
             clazz != Object.class;
             clazz = clazz.getSuperclass()) {
            try {
                method = clazz.getDeclaredMethod(methodName, parameterTypes);
                method.setAccessible(true);
                return method;
            } catch (Exception e) {
                // do nothing then can get method from super Class
            }
        }

        return null;
    }
}
