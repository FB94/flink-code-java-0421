package com.atguigu.day02.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random rand = new Random();

        String[] sensorlds = new String[10];
        double[] curFTemp = new double[10];

        for (int i = 0; i < 10; i++) {
            sensorlds[i] = "sensor_" + (i + 1);
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }
        while (running) {
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] += rand.nextGaussian() * 0.5;
                sourceContext.collect(new SensorReading(sensorlds[i], curTime, curFTemp[i]));
            }
            Thread.sleep(100);
        }

    }

    @Override
    public void cancel() {
        this.running = false;

    }
}
