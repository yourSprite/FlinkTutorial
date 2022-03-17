package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理word count
 *
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/15
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取数据
        DataSource<String> inputData = env.readTextFile("D:\\code\\flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\hello.txt");
        // 用faltmap处理数据，之后进行分组计算
        AggregateOperator<Tuple2<String, Integer>> result = inputData.flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1);
        // 打印结果
        result.print();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 空格分割
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
