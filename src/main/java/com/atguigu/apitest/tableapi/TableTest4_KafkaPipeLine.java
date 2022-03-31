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
public class TableTest4_KafkaPipeLine {
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

        // 2. 连接Kafka，读取数据
        String statement = "CREATE TABLE inputTable (" +
                "  id STRING," +
                "  `timestamp` BIGINT," +
                "  temp DOUBLE" +
                ")WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'sensor'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'csv'" +
                ")";
        tableEnv.executeSql(statement);

        // 3. 查询转换
        // 简单转换
        Table resultTable = tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_1'");
        tableEnv.toAppendStream(resultTable, Row.class).print("result");


        // 4. 建立kafka连接，输出到不同的topic下
        statement = "CREATE TABLE outputTable (" +
                "  id STRING," +
                "  temp DOUBLE" +
                ")WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'test'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'csv'" +
                ")";
        tableEnv.executeSql(statement);

        resultTable.executeInsert("outputTable");
    }
}
