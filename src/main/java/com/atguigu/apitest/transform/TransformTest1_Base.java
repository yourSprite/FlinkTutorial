package com.atguigu.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * <p>map 1对1
 * <p>faltmap 1对多
 * <p>fliter 多对1
 *
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/18
 */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // map，把String转换成长度输出
        SingleOutputStreamOperator<Object> mapStream = inputStream.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                return s.length();
            }
        });


        // flatmap，按逗号分字段
        SingleOutputStreamOperator<Object> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String s, Collector<Object> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });

        // filter, 筛选sensor_1开头的id对应的数据
        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });

        // 打印输出
        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();
    }
}
