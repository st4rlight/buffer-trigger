package com.github.phantomthief;

import java.util.concurrent.ConcurrentHashMap;

import com.github.phantomthief.collection.impl.BatchConsumeBlockingQueueTrigger;
import com.github.phantomthief.collection.impl.BatchConsumerTriggerBuilder;
import com.github.phantomthief.collection.impl.GenericBatchConsumerTriggerBuilder;
import com.github.phantomthief.simple.GenericSimpleBufferTriggerBuilder;
import com.github.phantomthief.simple.SimpleBufferTrigger;
import com.github.phantomthief.simple.SimpleBufferTriggerBuilder;

/**
 * BufferTrigger伴生类
 *
 * @author st4rlight <st4rlight@163.com>
 * Created on 2023-05-14
 */
public interface MoreBufferTrigger {

    /**
     * 快捷创建{@link GenericSimpleBufferTriggerBuilder}建造器，用于构造{@link SimpleBufferTrigger}实例.
     * <p>
     * 推荐用该方法构造{@link SimpleBufferTrigger}实例，适用用大多数场景
     *
     * @param <E> 元素类型
     * @param <C> 持有元素的容器类型，如使用默认容器实现（不指定 {@link GenericSimpleBufferTriggerBuilder#setContainerEx}），默认为 {@link ConcurrentHashMap#newKeySet()}，
     * <p>此时本泛型可以考虑设置为 <code>Set&lt;E&gt;</code>
     * <p>注意，强烈不建议使用默认容器，任何时候，都应该优先考虑自行设计容器类型!
     * @return {@link GenericSimpleBufferTriggerBuilder} 实例
     */
    static <E, C> GenericSimpleBufferTriggerBuilder<E, C> simple() {
        return new GenericSimpleBufferTriggerBuilder<>(new SimpleBufferTriggerBuilder<>());
    }

    /**
     * 该方法即将废弃，可更换为{@link #simple()}.
     */
    @Deprecated
    static SimpleBufferTriggerBuilder<Object, Object> simpleTrigger() {
        return SimpleBufferTrigger.newBuilder();
    }

    /**
     * 提供自带背压(back-pressure)的简单批量归并消费能力.
     * <p>
     *
     * <p>
     * FIXME: 高亮注意，目前 {@link #simple()} 也提供了背压能力 {@link GenericSimpleBufferTriggerBuilder#enableBackPressure()}
     * 所以本模式的意义已经小的多，如果没特殊情况，可以考虑都切换到 {@link SimpleBufferTrigger} 版本.
     */
    static <E> GenericBatchConsumerTriggerBuilder<E> batchBlocking() {
        return new GenericBatchConsumerTriggerBuilder<>(
                BatchConsumeBlockingQueueTrigger.newBuilder());
    }

    /**
     * 该方法即将废弃，请使用{@link #batchBlocking()}替代.
     */
    @Deprecated
    static BatchConsumerTriggerBuilder<Object> batchBlockingTrigger() {
        return BatchConsumeBlockingQueueTrigger.newBuilder();
    }
}
