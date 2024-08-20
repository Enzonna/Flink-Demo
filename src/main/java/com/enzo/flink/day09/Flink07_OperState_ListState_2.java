package com.enzo.flink.day09;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_OperState_ListState_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 开启检查点
        env.enableCheckpointing(5000L);

        DataStreamSource<String> socketDS = env.socketTextStream("fastfood102", 8888);

        // 在map算子中的每个并行度上计算数据的个数
        socketDS.process(
                new MyPro()
        ).print();
        env.execute();

    }
}


class MyPro extends ProcessFunction<String, String> implements CheckpointedFunction {

    // 存储每个并行子任务计算元素的个数
    Integer count = 0;

    // 定义一个算子状态，用于对成员变量进行备份和恢复
    ListState<Integer> countListState;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState✅✅✅");
        countListState.clear();
        countListState.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState✅✅✅");
        // 注意：算子状态和键控状态不一样
        // 键控状态的初始化是在open方法中完成的 getRuntimeContext.getXxxState
        // 算子状态的初始化是在initializeState方法中完成的 context.getOperatorStateStore().getXxxState
        countListState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<Integer>("countListState", Integer.class));

        // 如果是故障恢复，则从算子状态中获取数据
        if (context.isRestored()) {
            count = countListState.get().iterator().next();
        }
    }

    @Override
    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
        out.collect("当前子任务" + getRuntimeContext().getIndexOfThisSubtask() + "计算了" + ++count + "个元素");
    }


}