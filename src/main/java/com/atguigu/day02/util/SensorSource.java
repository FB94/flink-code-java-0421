package com.atguigu.day02.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random rand = new Random();

        String[] sendorIds = new String[10];
        double[] curFTemp = new double[10];
        for(int i = 0)

    }
}
