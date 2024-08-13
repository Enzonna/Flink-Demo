
并行度
    当要处理的数据量非常大时，我们可以把一个算子操作，“复制”多份到多个节点，数据来了之后就可以到其中任意一个执行。
    这样一来，一个算子任务就被拆分成了多个并行的“子任务”（subtasks）
    一个特定算子的子任务（subtask）的个数被称之为其并行度（parallelism）
    一个Flink应用程序的并行度取决于并行度最大的算子的并行度个数

    默认情况下，如果在本地(Idea)中运行程序，如果没有指定并行度，并行度的个数为当前CPU的线程数

如何设置并行度
    在代码中全局设置
        env.setParallelism(3)
    针对某一个算子单独设置并行度
        算子.setParallelism(3)
    在flink的配置文件  flink-conf.yaml
        parallelism.default: 1
    在提交作业的时候通过-p参数指定

并行度优先级
    算子单独设置并行度  > 全局设置 > 在提交作业的时候通过-p参数指定 > 在flink的配置文件

算子链
    在Flink中，并行度相同的一对一（one to one）算子操作，可以直接链接在一起形成一个“大”的任务（task）
    这样原来的算子就成为了真正任务里的一部分，这样的技术被称为“算子链”（Operator Chain）
    算子链是Flink提供一种非常有效的优化手段，不需要我们做什么处理，默认就会进行合并
    前提：
        算子间必须是one-to-one的关系
            并行度相同
            不能存在重分区操作
禁用算子链
    全局禁用算子链 env.disableOperatorChaining();
    算子禁用  算子.disableChaining()
    开始新链  算子.startNewChain()

任务槽
    Flink程序在执行的时候，会被划分为多个算子子任务，每个子任务在执行的时候，需要TaskManager提供资源
    TaskManager是一个JVM进程，开始开启多个线程执行任务，每一个线程叫做任务槽
        任务槽可以均分TaskManager的内存资源
        在flink-conf.yaml的配置文件中可以设置一个tm上slot的数量
            taskmanager.numberOfTaskSlots: 1
        在设置slot数量的时候，除了要考虑内存资源外，也需要考虑CPU情况，一般slot的数量和cpu核数之间的关系是1:1或者2:1
slot共享
    只要属于同一个作业，那么对于不同任务节点（算子）的并行子任务，就可以放到同一个slot上执行
    一个Flink应用执行需要的Slot的数量 = 各个共享组中并行度最大的算子并行度个数之和

四张图
    逻辑流程---程序执行拓扑
    作业图 ---合并算子链
    执行图 ---作业图的并行化版本
    物理"图" --- 程序的执行过程

以Yarn的应用模式为例描述Flink作业提交流程





