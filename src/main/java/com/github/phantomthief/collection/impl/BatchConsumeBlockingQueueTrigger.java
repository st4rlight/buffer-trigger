package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.concurrent.MoreFutures.scheduleWithDynamicDelay;
import static com.github.phantomthief.util.MoreLocks.runWithLock;
import static com.github.phantomthief.util.MoreLocks.runWithTryLock;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.Integer.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.DAYS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.github.phantomthief.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.util.concurrent.Uninterruptibles;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link BufferTrigger}基于阻塞队列的批量消费触发器实现.
 * <p>
 * 该触发器适合生产者-消费者场景，缓存容器基于{@link LinkedBlockingQueue}队列实现.
 * <p>
 * 触发策略类似Kafka linger，批处理阈值与延迟等待时间满足其一即触发消费回调.
 * @author w.vela
 */
@Slf4j
public class BatchConsumeBlockingQueueTrigger<E> implements BufferTrigger<E> {

    // 具体的buffer容器
    private final BlockingQueue<E> queue;
    // 批量大小
    private final int batchSize;

    // 具体的消费逻辑，支持链式消费
    private final ThrowableConsumer<List<E>, Exception> consumer;
    // 消费异常处理器
    private final BiConsumer<Throwable, List<E>> exceptionHandler;
    // 定时任务执行线程池
    private final ScheduledExecutorService scheduledExecutorService;

    // 加锁确保同时只有一个线程可以改running flag
    private final ReentrantLock lock = new ReentrantLock();
    // 指示当前是否正在消费
    private final AtomicBoolean running = new AtomicBoolean();

    // 关闭标记，当检测到该标记时，其他方法不应该再进行操作
    private volatile boolean shutdown;
    // 关闭操作，一般用来清理资源
    private final Runnable shutdownExecutor;


    BatchConsumeBlockingQueueTrigger(BatchConsumerTriggerBuilder<E> builder) {
        Supplier<Duration> linger = builder.linger;
        this.batchSize = builder.batchSize;
        this.queue = new LinkedBlockingQueue<>(max(builder.bufferSize, batchSize));
        this.consumer = builder.consumer;
        this.exceptionHandler = builder.exceptionHandler;
        this.scheduledExecutorService = builder.scheduledExecutorService;

        // 开始定时任务，linger动态可调整
        Future<?> future = scheduleWithDynamicDelay(scheduledExecutorService, linger, () -> doBatchConsumer(TriggerType.LINGER));
        this.shutdownExecutor = () -> {
            future.cancel(false);
            if (builder.usingInnerExecutor) {
                shutdownAndAwaitTermination(builder.scheduledExecutorService, 1, DAYS);
            }
        };
    }

    /**
     * 该方法即将废弃，可更换为{@link com.github.phantomthief.MoreBufferTrigger#batchBlocking}.
     */
    @Deprecated
    public static BatchConsumerTriggerBuilder<Object> newBuilder() {
        return new BatchConsumerTriggerBuilder<>();
    }

    /**
     * 将需要定时处理的元素推入队列.
     * <p>
     * 新元素入队列后，会检测一次当前队列元素数量，如满足批处理阈值，会触发一次消费回调.
     */
    @Override
    public void enqueue(E element) {
        // 关闭标记检测
        checkState(!shutdown, "buffer trigger was shutdown.");

        // 一直阻塞直到成功
        Uninterruptibles.putUninterruptibly(queue, element);
        tryTrigBatchConsume();
    }

    private void tryTrigBatchConsume() {
        if (this.queue.size() >= this.batchSize) {
            runWithTryLock(lock, () -> {
                if (queue.size() >= batchSize) {
                    // 这里检测状态是因为这里是异步消费任务
                    // 并发时，可能会一下子触发多个任务，实际上没必要
                    if (!running.get()) {
                        this.scheduledExecutorService.execute(() -> doBatchConsumer(TriggerType.ENQUEUE));
                        running.set(true);
                    }
                }
            });
        }
    }

    @Override
    public void manuallyDoTrigger() {
        doBatchConsumer(TriggerType.MANUALLY);
    }

    private void doBatchConsumer(TriggerType type) {
        runWithLock(lock, () -> {
            try {
                running.set(true);
                int queueSizeBeforeConsumer = queue.size();
                int consumedSize = 0;

                while (!queue.isEmpty()) {
                    if (queue.size() < batchSize) {
                        if (type == TriggerType.ENQUEUE) {
                            // 数量不够，若是ENQUEUE触发则直接返回
                            return;
                        } else if (type == TriggerType.LINGER && consumedSize >= queueSizeBeforeConsumer) {
                            // 数量不够，若是LINGER触发，且已经消费超过之前的队列大小，则也直接返回
                            return;
                        }
                    }

                    // 触发的数量要么是batch size，要么是当前的实际数量
                    List<E> toConsumeData = new ArrayList<>(min(batchSize, queue.size()));
                    queue.drainTo(toConsumeData, batchSize);

                    if (!toConsumeData.isEmpty()) {
                        if (log.isDebugEnabled()) {
                            log.debug("do batch consumer:{}, size:{}", type, toConsumeData.size());
                        }
                        consumedSize += toConsumeData.size();
                        doConsume(toConsumeData);
                    }
                }
            } finally {
                running.set(false);
            }
        });
    }

    private void doConsume(List<E> toConsumeData) {
        try {
            this.consumer.accept(toConsumeData);
        } catch (Throwable e) {
            if (this.exceptionHandler != null) {
                try {
                    this.exceptionHandler.accept(e, toConsumeData);
                } catch (Throwable ex) {
                    log.error("", ex);
                }
            } else {
                log.error("Ops.", e);
            }
        }
    }

    @Override
    public long getPendingChanges() {
        return queue.size();
    }

    @Override
    public void close() {
        shutdown = true;
        try {
            manuallyDoTrigger();
        } finally {
            shutdownExecutor.run();
        }
    }

    /**
     * just for log and debug
     */
    private enum TriggerType {
        LINGER, ENQUEUE, MANUALLY
    }
}
