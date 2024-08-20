package com.enzo.flink.day09;

import com.enzo.flink.bean.WaterSensor;
import com.enzo.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 水位超过指定的阈值发送告警，阈值可以动态修改
 */
public class Flink08_OpeState_broadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // 内存计算，读写速度非常快；但是，状态的大小会受到集群可用内存的限制，如果应用的状态随着时间不停地增长，就会耗尽内存资源
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // 硬盘存储，所以可以根据可用的磁盘空间进行扩展，所以它非常适合于超级海量状态的存储。
        // 不过由于每个状态的读写都需要做序列化/反序列化，而且可能需要直接从磁盘读取数据，这就会导致性能的降低，平均读写性能要比HashMapStateBackend慢一个数量级

        // 从指定的网络端口读取水位信息
        SingleOutputStreamOperator<WaterSensor> wsDS = env
                .socketTextStream("fastfood102", 8888)
                .map(
                        new WaterSensorMapFunction()
                );

        // 从指定的网络端口读取阈值信息
        SingleOutputStreamOperator<Integer> thresholdDS = env
                .socketTextStream("fastfood102", 8889)
                .map(Integer::valueOf);

        // 广播阈值流
        // 注意：广播状态的使用方式是固定的，在对流中数据进行广播的时候，需要在broadcast方法中传递状态描述器，在描述状态中存储的数据结构
        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("threshold", String.class, Integer.class);
        BroadcastStream<Integer> broadcastDS = thresholdDS.broadcast(mapStateDescriptor);

        // 将主流（水位信息）和广播流（阈值信息）进行关联
        BroadcastConnectedStream<WaterSensor, Integer> connectDS = wsDS.connect(broadcastDS);

        // 对连接后的数据进行处理 - process
        // processElement : 处理主流数据，从状态中获取阈值信息，来判断是否超过了警戒线
        // processBroadcastElement : 处理广播数据，将广播流中的阈值信息放到状态中存储起来
        SingleOutputStreamOperator<String> processDS = connectDS.process(
                new BroadcastProcessFunction<WaterSensor, Integer, String>() {
                    @Override
                    public void processElement(WaterSensor ws, BroadcastProcessFunction<WaterSensor, Integer, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 获取当前采集的水位信息
                        Integer vc = ws.getVc();
                        // 获取广播状态,在处理主流数据的时候，只能使用广播状态，不能对其进行修改
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        // 从广播状态中获取阈值信息
                        Integer threshold = broadcastState.get("threshold") == null ? 100000 : broadcastState.get("threshold");
                        if (vc > threshold) {
                            out.collect(ws.getId() + "水位超过警戒线，当前水位：" + vc + "，警戒线：" + threshold + "❌❌❌");
                        }
                    }

                    @Override
                    public void processBroadcastElement(Integer threshold, BroadcastProcessFunction<WaterSensor, Integer, String>.Context ctx, Collector<String> out) throws Exception {

                        // 获取广播状态
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        // 将阈值信息放到广播状态中
                        broadcastState.put("threshold", threshold);

                    }
                }
        );

        processDS.print();

        env.execute();
    }
}





















