package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new MinAgg())
                .print();

        env.execute();
    }

    public static class MinAgg implements AggregateFunction<SensorReading, Tuple2<String, Double>, Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> createAccumulator() {
            return Tuple2.of("", Double.MAX_VALUE);
        }

        @Override
        public Tuple2<String, Double> add(SensorReading r, Tuple2<String, Double> acc) {
            if (r.temperature < acc.f1) {
                return Tuple2.of(r.id, r.temperature);
            } else {
                return acc;
            }
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple2<String, Double> acc) {
            return acc;
        }

        //merge 函数什么时候需要实现？
        //两个条件：1.事件事件 2.会话时间
        @Override
        public Tuple2<String, Double> merge(Tuple2<String, Double> acc1, Tuple2<String, Double> acc2) {
            return null;
        }
    }

}
