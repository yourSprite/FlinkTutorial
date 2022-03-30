package com.atguigu.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/30
 */
public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        // 2. 表的创建：连接外部系统，读取数据
        String statement = "CREATE TABLE inputTable (" +
                "  id STRING," +
                "  `timestamp` BIGINT," +
                "  temp DOUBLE" +
                ")WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt'," +
                "  'format' = 'csv'" +
                ")";
        blinkStreamTableEnv.executeSql(statement);

        // 3. 查看表结构
        Table inputTable = blinkStreamTableEnv.from("inputTable");
        inputTable.printSchema();

        // 4. 简单查询
        Table resultTable = blinkStreamTableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_1'");
        blinkStreamTableEnv.toAppendStream(resultTable, Row.class).print("result");

        // 5.聚合统计
        Table sqlAggTable = blinkStreamTableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");
        blinkStreamTableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlagg");

        env.execute();
    }
}
