package com.atguigu.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/31
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        // 2. 将流转换成表，定义时间特性
        // " pt AS PROCTIME() " + -- 定义处理时间
//        " rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ), " +
//                " watermark for rt as rt - interval '1' second" + -- 定义事件时间
        String stetement =
                "create table sensor (" +
                        " id STRING, " +
                        " ts BIGINT, " +
                        " temperature DOUBLE, " +
                        " rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ), " +
                        " watermark for rt as rt - interval '1' second" +
                        ") with (" +
                        " 'connector.type' = 'filesystem', " +
                        " 'connector.path' = 'D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt', " +
                        " 'format.type' = 'csv')";

        tableEnv.executeSql(stetement);
        // 3. 窗口操作
        // 3.1 Group Window（10s 滑动窗口）
        Table resultSqlTable = tableEnv.sqlQuery(
                "select id, " +
                        "count(id) as cnt," +
                        "avg(temperature) as avgTemp," +
                        "tumble_end(rt, interval '10' second) " +
                        "from sensor " +
                        "group by id, tumble(rt, interval '10' second)");

        // 3.2 Over Window
        Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temperature) over ow " +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");

//        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");
        tableEnv.toRetractStream(overSqlResult, Row.class).print("sql");

        env.execute();
    }
}
