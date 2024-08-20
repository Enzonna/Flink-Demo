

以滚动事件时间窗口为例
    窗口对象什么时候创建
        当属于这个窗口的第一个元素到来的时候创建窗口对象
    窗口起始时间
        向下取整的算法
    窗口的结束时间
        起始时间 + 窗口大小
    窗口最大时间
        maxTimestamp = 结束时间 - 1ms
    窗口什么时候触发计算
        水位线到了窗口的最大时间
        window.maxTimestamp() <= ctx.getCurrentWatermark()
    窗口什么时候关闭
        水位线到了  window.maxTimestamp() + allowedLateness

迟到数据的处理
    指定水位线的生成策略为有界乱序，指定乱序程度
    在开窗的时候，指定窗口的允许迟到时间
    侧输出流

基于时间双流join
    基于窗口
        滚动
        滑动
        会话
    基于状态
        IntervalJoin
        基本语法：
            keyedA
                .intervalJoin(keyedB)
                .between(下界,上界)
                .process()
        底层实现:
            connect + 状态

        具体处理步骤
            判断是否迟到
            将当前数据放到状态中缓存起来
            用当前数据和另外一条流中缓存的数据进行关联
            清状态
    局限性：只支持内连接



