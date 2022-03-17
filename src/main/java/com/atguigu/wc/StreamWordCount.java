package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理word count
 *
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/15
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从文本读取数据
        // DataStreamSource<String> inptData = env.readTextFile("D:\\code\\flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\hello.txt");

        // 用parameter tool工具从程序启动参数中提取配置
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket读取数据
        DataStreamSource<String> inptData = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultData = inptData.flatMap(new MyFlatMapFunction())
                .keyBy(0)
                .sum(1);

        resultData.print();


        env.execute();

    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}