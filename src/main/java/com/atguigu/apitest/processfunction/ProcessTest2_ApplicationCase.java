package com.atguigu.apitest.processfunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author wangyutian
 * @version 1.0
 * @date 2022/3/28
 */
public class ProcessTest2_ApplicationCase {
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

        // 测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy(SensorReading::getId)
                .process(new TempConsIncreWarning(10))
                .print();

        env.execute();
    }

    // 实现自定义处理函数，检测一段时间内的温度连续上升，输出报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<String, SensorReading, String> {
        // 定义私有属性，当前统计的时间间隔
        private int interval;

        public TempConsIncreWarning(int interval) {
            this.interval = interval;
        }

        // 定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            // 判断lastTempState中状态不为空
            if (lastTemp == null) {
                lastTempState.update(value.getTemperature());
            } else {
                if (value.getTemperature() > lastTemp && timerTs == null) {
                    // 计算时间戳并注册定时器
                    long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    // 更新定时状态
                    timerTsState.update(ts);
                } else if (value.getTemperature() <= lastTemp && timerTs != null) {
                    // 删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    timerTsState.clear();
                }
                // 更新温度状态
                lastTempState.update(value.getTemperature());
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) {
            // 定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey() + "温度值连续" + interval + "s上升");
            timerTsState.clear();
        }

        @Override
        public void close() {
            lastTempState.clear();
            timerTsState.clear();
        }
    }
}
