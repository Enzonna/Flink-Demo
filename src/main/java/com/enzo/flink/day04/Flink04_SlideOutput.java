package com.enzo.flink.day04;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 侧输出流
 */
public class Flink04_SlideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> socketDS = env.socketTextStream("fastfood102", 8888);

        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // 1. 定义侧输出流标签
        // ❌❌❌注意类型擦除！！！
        // 匿名内部类创建
        OutputTag<WaterSensor> ws1Tag = new OutputTag<WaterSensor>("ws1Tag") {
        };
        // 创建对象的时候，传第二个参数指定类型
        OutputTag<WaterSensor> ws2Tag = new OutputTag<WaterSensor>("ws2Tag", Types.POJO(WaterSensor.class));


        // 2.分流
        // 注意：如果是侧输出流，只能调用process算子对流中数据进行处理
        // 因为只有process算子在方法中能获取到context，需要通过上下文对象操作侧输出流
        SingleOutputStreamOperator<WaterSensor> process = wsDS.process(
                new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor ws, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        String id = ws.getId();
                        if (id.equals("ws1")) {
                            // 将ws1数据放到侧输出流1
                            ctx.output(ws1Tag, ws);
                        } else if (id.equals("ws2")) {
                            // 将ws2数据放到侧输出流2
                            ctx.output(ws2Tag, ws);
                        } else {
                            // 将其他数据放到主流
                            out.collect(ws);
                        }
                    }
                }
        );

        process.print("主流 -> ");
        process.getSideOutput(ws1Tag).print("ws1Tag -> ");
        process.getSideOutput(ws2Tag).print("ws2Tag -> ");

        env.execute();
    }

}
