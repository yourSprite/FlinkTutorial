package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/18
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // input数据转换成pojo
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        // 分组
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合，获取当前最大温度
        SingleOutputStreamOperator<SensorReading> result = keyedStream.maxBy("temperature");

        result.print();

        env.execute();
    }
}
