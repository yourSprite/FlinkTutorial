package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1.侧输出流
 * 2.connect合并流（2条），数据类型可以不同
 * 3.union合并流（多条），数据类型必须相同
 *
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/20
 */

public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 实现自定义富函数类
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = dataStream.map(new RichMapFunction<SensorReading, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
            }

            // 初始化工作，一般是定义状态，或者建立数据库连接
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open");
            }

            // 一般是关闭连接和清空状态的收尾操作
            @Override
            public void close() throws Exception {
                System.out.println("close");
            }
        });


        resultStream.print();

        env.execute();
    }

    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }
}
