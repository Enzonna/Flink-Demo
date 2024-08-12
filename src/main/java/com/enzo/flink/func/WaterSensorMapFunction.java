package com.enzo.flink.func;

import com.enzo.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String s) throws Exception {
        String[] filedArr = s.split(",");
        return new WaterSensor(filedArr[0], Long.valueOf(filedArr[1]), Integer.valueOf(filedArr[2]));
    }
}
