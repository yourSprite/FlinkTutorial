package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/24
 */
public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开计数窗口测试
        SingleOutputStreamOperator<Double> avgTempResultStream = dataStream
                .keyBy(SensorReading::getId)
                .countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                        return new Tuple2<>(value.getTemperature() + accumulator.f0, accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });

        avgTempResultStream.print();

        env.execute();
    }
}
