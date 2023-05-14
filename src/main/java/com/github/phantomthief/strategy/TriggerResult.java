package com.github.phantomthief.strategy;

import static java.util.concurrent.TimeUnit.DAYS;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 触发器执行结果，计划任务内部使用，一般无需关注
 *
 * @author st4rlight <st4rlight@163.com>
 * Created on 2023-05-14
 */
@Getter
@AllArgsConstructor
public class TriggerResult {

    // 是否触发消费
    private final boolean doConsume;
    // 下一次触发间隔
    private final long nextPeriod;

    // 空白实例
    private static final TriggerResult EMPTY =
            new TriggerResult(false, DAYS.toMillis(1));


    public static TriggerResult of(boolean doConsumer, long nextPeriod) {
        return new TriggerResult(doConsumer, nextPeriod);
    }

    public static TriggerResult empty() {
        return EMPTY;
    }
}
