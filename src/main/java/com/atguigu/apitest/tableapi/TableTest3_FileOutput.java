package com.atguigu.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/31
 */
public class TableTest3_FileOutput {
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

        // 2. 表的创建：连接外部系统，读取数据
        // 读取文件
        String statement = "CREATE TABLE inputTable (" +
                "  id STRING," +
                "  `timestamp` BIGINT," +
                "  temp DOUBLE" +
                ")WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt'," +
                "  'format' = 'csv'" +
                ")";
        tableEnv.executeSql(statement);

        // 3. 查询转换
        Table resultTable = tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_1'");

        // 4. 输出到文件
        // 连接外部文件注册输出表
        statement = "CREATE TABLE outputTable (" +
                "  id STRING," +
                "  temp DOUBLE" +
                ")WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\output'," +
                "  'format' = 'csv'" +
                ")";
        tableEnv.executeSql(statement);
        resultTable.executeInsert("outputTable");
    }
}
