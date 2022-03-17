package com.atguigu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStreamSource<String> dataStream = env.readTextFile("D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        dataStream.print();

        env.execute();
    }
}
