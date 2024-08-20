检查点相关的配置
    启用检查点
    检查点存储
    检查点模式（CheckpointingMode）
    超时时间（checkpointTimeout）
    最小间隔时间（minPauseBetweenCheckpoints）
    最大并发检查点数量（maxConcurrentCheckpoints）
    开启外部持久化存储（enableExternalizedCheckpoints）--job取消后，检查点是否保留
    检查点连续失败次数（tolerableCheckpointFailureNumber）
    非对齐检查点（enableUnalignedCheckpoints）
    对齐检查点超时时间（alignedCheckpointTimeout）


    通用增量 checkpoint (changelog)
    最终检查点
检查点配置位置
    在flink-conf.yaml文件中
    在代码中通过API指定
保存点
    底层原理实现和检查点一样
    检查点是程序自动进行快照
    保存点是程序员手动进行快照
        --必须指定uid
一致性级别
    最多一次（At-Most-Once）
    至少一次（At-Least-Once）
    精确一次（Exactly-Once）

Flink的端到端一致性
    Source(读取数据的外部系统)
        可重置偏移量
    Flink流处理框架本身
        检查点
    Sink(写入数据的外部系统)
        幂等
            外部系统必须支持幂等
        事务
            WAL:外部系统不支持事务
            *2PC:外部系统支持事务