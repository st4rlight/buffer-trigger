package com.github.phantomthief.strategy;

/**
 * 触发消费策略接口.
 * <p>
 * 如需自定义消费策略，请参考{@link MultiIntervalTriggerStrategy}代码
 */
public interface TriggerStrategy {

    /**
     * 获取触发器执行结果，用于判断是否可执行消费回调
     *
     * @param lastConsumeTimestamp 上一次消费时间
     * @param changedCount 本次积累了几个元素
     */
    TriggerResult canTrigger(long lastConsumeTimestamp, long changedCount);
}
