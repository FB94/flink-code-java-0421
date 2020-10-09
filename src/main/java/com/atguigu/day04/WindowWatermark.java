package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WindowWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream
                //Tuple2<key,timestamp>，毫秒时间戳
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                //默认200ms（机器时间）插入一次水位线
                .assignTimestampsAndWatermarks(
                        //为乱序事件流，插入水位线
                        WatermarkStrategy
                                //最大延迟时间是5s
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> r, long l) {
                                        return r.f1;//告诉系统元素的事件时间戳是r.f1字段
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<Object> collector) throws Exception {
                        Long count = 0L;
                        for (Tuple2<String, Long> i : iterable) {
                            count += 1;
                        }
                        collector.collect("窗口中共有" + count + "条元素");
                    }
                })
                .print();


        env.execute();

    }
}
