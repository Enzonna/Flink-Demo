package com.enzo.flink.func;

import com.enzo.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class MyMapFunction implements MapFunction<WaterSensor, String> {
    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.id;
    }
}