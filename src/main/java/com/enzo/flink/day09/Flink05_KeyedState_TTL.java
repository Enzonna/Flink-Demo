package com.enzo.flink.day09;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_KeyedState_TTL {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 12345);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // 按照传感器的id分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);


        keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 1. 声明键控状态，注意和普通变量作用范围不同，键控状态的作用范围就是keyBy后的每个组
                    // 在成员变量位置
                    ValueState<Integer> lastVcState;


                    // 注意：不能在声明的时候直接赋值，因为这个时候还获取不到运行时上下文
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 2. 状态赋值,在open方法中
                        ValueStateDescriptor<Integer> valueStateDescriptor
                                = new ValueStateDescriptor<Integer>("lastVcState", Integer.class);

                        // 启用TTL
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig
                                // 注意：这里的时间指的是系统时间
                                .newBuilder(Time.seconds(10))
                                /*
                                    指定了什么时候更新状态失效时间
                                 */
                                // OnCreateAndWrite 只有创建状态和更改状态（写操作）时更新失效时间，默认方法
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                // OnReadAndWrite 无论读写操作都会更新失效时间，也就是只要对状态进行了访问，就表明它是活跃的，从而延长生存时间
                                // .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)

                                /*
                                    所谓的“状态可见性”，是指因为清除操作并不是实时的，
                                    所以当状态过期之后还有可能继续存在，这时如果对它进行访问，能否正常读取到就是一个问题了
                                 */
                                // NeverReturnExpired 状态过期后，不会返回过期状态，而是返回null，默认方法
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                // ReturnExpireDefNotCleanedUp 状态过期后，返回过期状态，但是不会清除状态
                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                                .build());

                        // 获取状态,状态描述器
                        lastVcState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前水位值
                        Integer curVc = ws.getVc();
                        // 3. 在处理函数中使用状态，获取上次水位值
                        Integer lastVc = lastVcState.value() == null ? 0 : lastVcState.value();

                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("传感器" + ctx.getCurrentKey() + "当前水位" + curVc + "与上一个水位" + lastVc + "差值超过10");
                        }

                        if (curVc < 30) {
                            // 更新当前水位值到状态中
                            lastVcState.update(curVc);                        // 更新当前水位值到状态中
                        }
                    }
                }
        ).printToErr();


        env.execute();

    }
}
