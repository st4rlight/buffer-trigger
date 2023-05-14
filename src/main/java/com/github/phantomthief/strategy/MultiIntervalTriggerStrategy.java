package com.github.phantomthief.strategy;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.MapUtils;

import com.github.phantomthief.simple.SimpleBufferTrigger;
import com.github.phantomthief.simple.SimpleBufferTriggerBuilder;

import lombok.Getter;

/**
 * trigger like redis's rdb
 *
 * save 900 1
 * save 300 10
 * save 60 10000
 *
 * 注意，这里的触发条件和 Kafka Producer 的 linger 不一样，并不是 时间和次数达到一个就触发一次
 * 而是指定时间内累计到特定次数才触发，所以一般会有一个兜底的 1 次来做时间触发
 *
 * 更进一步，我们的场景大多数时候一般使用 {@link SimpleBufferTriggerBuilder#interval} 触发就能满足业务的基本需求
 * 所以目前 {@link SimpleBufferTrigger} 没有提供类 Kafka Producer 的 linger 触发模式；
 *
 * @author w.vela
 * Created on 15/07/2016.
 */
public class MultiIntervalTriggerStrategy implements TriggerStrategy {

    @Getter
    private long minTriggerPeriod = Long.MAX_VALUE;
    // time（millis） --> count，从小到大排序
    private final SortedMap<Long, Long> triggerMap = new TreeMap<>();

    /**
     * 支持链式配置
     */
    public MultiIntervalTriggerStrategy on(long interval, TimeUnit unit, long count) {
        long intervalInMs = unit.toMillis(interval);
        triggerMap.put(intervalInMs, count);
        // 计算所需要设置的最小定时任务周期
        minTriggerPeriod = checkAndCalcMinPeriod();
        return this;
    }

    /**
     * 计算定时任务触发的最小间隔
     */
    private long checkAndCalcMinPeriod() {
        if (MapUtils.isEmpty(triggerMap)) {
            throw new IllegalStateException("no trigger setting");
        }

        long minPeriod = Long.MAX_VALUE;
        long maxTrigChangeCount = Long.MAX_VALUE;
        long lastPeriod = 0;

        for (Entry<Long, Long> entry : triggerMap.entrySet()) {
            long period = entry.getKey();

            // minPeriod要么取最小的时间周期，要么取最小间隔
            minPeriod = min(minPeriod, period);
            if (lastPeriod > 0) {
                minPeriod = min(minPeriod, period - lastPeriod);
            }

            // 检测配置是否异常，后面时间长的触发个数，不能大于等于前面的触发个数（这样子配置没意义）
            long trigChangedCount = entry.getValue();
            if (maxTrigChangeCount <= trigChangedCount) {
                throw new IllegalArgumentException("found invalid trigger setting:" + triggerMap);
            } else {
                maxTrigChangeCount = trigChangedCount;
            }

            lastPeriod = period;
        }

        return minPeriod;
    }

    @Override
    public TriggerResult canTrigger(long lastConsumeTimestamp, long changedCount) {
        checkArgument(!triggerMap.isEmpty());

        boolean doConsume = false;
        long now = currentTimeMillis();

        for (Entry<Long, Long> entry : triggerMap.entrySet()) {
            // 触发间隔未到，检测下一个
            if (now - lastConsumeTimestamp < entry.getKey()) {
                // 实际上treeMap实现从小到大，这里也可以直接break
                continue;
            }
            if (changedCount >= entry.getValue()) {
                doConsume = true;
                break;
            }
        }

        // 下一次触发时间固定最小检测间隔
        return TriggerResult.of(doConsume, minTriggerPeriod);
    }
}
