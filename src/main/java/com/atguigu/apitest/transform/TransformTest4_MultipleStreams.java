package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 1.侧输出流
 * 2.connect合并流（2条），数据类型可以不同
 * 3.union合并流（多条），数据类型必须相同
 *
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/19
 */
public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 使用侧输出流进行分流，此版本split已移除
        SingleOutputStreamOperator<Object> processStream = dataStream.process(new ProcessFunction<SensorReading, Object>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, Object>.Context ctx, Collector<Object> out) {
                if (value.getTemperature() > 30) {
                    ctx.output(new OutputTag<SensorReading>("high") {
                    }, value);
                } else {
                    ctx.output(new OutputTag<SensorReading>("low") {
                    }, value);
                }
            }
        });

        processStream.getSideOutput(new OutputTag<SensorReading>("high") {
        }).print("high");
        processStream.getSideOutput(new OutputTag<SensorReading>("low") {
        }).print("low");

        // 合流connect，姜高温流转换成二院组类型，与低温流合并之后，输出状态信息
        DataStream<SensorReading> highTempStream = processStream.getSideOutput(new OutputTag<SensorReading>("high") {
        });
        DataStream<SensorReading> lowTempStream = processStream.getSideOutput(new OutputTag<SensorReading>("low") {
        });

        // 高温流转换二元组
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<String, Double>(value.getId(), value.getTemperature());
            }
        });

        // 合流
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = warningStream.connect(lowTempStream);

        // coMap
        SingleOutputStreamOperator<Object> resultStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print("resultStream");

        // union 联合多条流，数据类型要相同
        DataStream<SensorReading> allTempStream = highTempStream.union(lowTempStream);
        allTempStream.print("allTempStream");

        env.execute();
    }
}
