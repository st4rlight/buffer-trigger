package com.github.phantomthief.simple;

import static com.github.phantomthief.constant.TriggerConstant.DEFAULT_NEXT_TRIGGER_PERIOD;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import static java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import com.github.phantomthief.BufferTrigger;
import com.github.phantomthief.MoreBufferTrigger;
import com.github.phantomthief.backpressure.BackPressureHandler;
import com.github.phantomthief.backpressure.GlobalBackPressureListener;
import com.github.phantomthief.strategy.TriggerResult;
import com.github.phantomthief.strategy.TriggerStrategy;
import com.github.phantomthief.support.NameRegistry;
import com.github.phantomthief.support.RejectHandler;
import com.github.phantomthief.util.ThrowableConsumer;

import cn.st4rlight.util.value.MoreNumber;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link BufferTrigger}的通用实现，适合大多数业务场景
 * <p>
 * 消费触发策略会考虑消费回调函数的执行时间，实际执行间隔 = 理论执行间隔 - 消费回调函数执行时间；
 * 如回调函数执行时间已超过理论执行间隔，将立即执行下一次消费任务.
 *
 * @param <E> 元素类型，即要入队的的东西
 * @param <C> 容器类型，可以自定义使用线程安全的容器
 *
 * @author w.vela
 */
@Slf4j
public class SimpleBufferTrigger<E, C> implements BufferTrigger<E> {

    // 计划任务线程名称
    private final String name;

    // 缓存阈值（非严格计数，可能会略微超过阈值）
    private final LongSupplier maxCountSupplier;
    // 原子计数，指示当前有多少个待消费元素
    private final AtomicLong counter = new AtomicLong();
    // 容量满了之后的拒绝回调
    private final RejectHandler<E> rejectHandler;

    // 实际队列
    private final AtomicReference<C> buffer = new AtomicReference<>();
    // 用来提供容器的工厂
    private final Supplier<C> bufferFactory;
    // 执行入队的方法，需要返回本次入队的个数
    private final ToIntBiFunction<C, E> queueAdder;
    // 具体的消费逻辑，支持链式消费
    private final ThrowableConsumer<C, Throwable> consumer;
    // 记录上一次消费时间（初始化为当前时间）
    private volatile long lastConsumeTimestamp = currentTimeMillis();
    // 消费异常处理器
    private final BiConsumer<Throwable, C> exceptionHandler;

    // 读写锁和条件，以及一个锁条件，目前看来锁的buffer对象（实际上也是在确保buffer、counter合为一个原子操作）
    private final ReadLock readLock;
    private final WriteLock writeLock;
    private final Condition writeCondition;

    // 关闭标记，当检测到该标记时，其他方法不应该再进行操作
    private volatile boolean shutdown;
    // 关闭操作，一般用来清理资源
    private final Runnable shutdownExecutor;



    /**
     * 使用提供的构造器创建SimpleBufferTrigger实例
     */
    SimpleBufferTrigger(SimpleBufferTriggerBuilder<E, C> builder) {
        this.name = builder.bizName;
        this.queueAdder = builder.queueAdder;
        this.bufferFactory = builder.bufferFactory;
        this.consumer = builder.consumer;
        this.exceptionHandler = builder.exceptionHandler;
        this.maxCountSupplier = builder.maxBufferCount;
        this.rejectHandler = builder.rejectHandler;
        this.buffer.set(this.bufferFactory.get());

        // 可以控制是否需要锁（在高并发下是需要的，否则可能会丢数据）
        if (!builder.disableSwitchLock) {
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            readLock = lock.readLock();
            writeLock = lock.writeLock();
            writeCondition = writeLock.newCondition();
        } else {
            readLock = null;
            writeLock = null;
            writeCondition = null;
        }

        // 开启一个触发runnable，runnable内会调度下一个runnable
        TriggerRunnable triggerRunnable =
                new TriggerRunnable(builder.scheduledExecutorService, builder.triggerStrategy, this);
        // 默认第一个检查1s后执行
        builder.scheduledExecutorService.schedule(triggerRunnable, DEFAULT_NEXT_TRIGGER_PERIOD, MILLISECONDS);

        // 清理资源，主要是清理线程池，默认是最多等1天
        this.shutdownExecutor = () -> {
            if (builder.usingInnerExecutor) {
                // 注意：该方法目前无一些特殊处理，默认一般都是可以成功的，业务上应该避免写出长时间无法结束的消费runnable
                shutdownAndAwaitTermination(builder.scheduledExecutorService, 1, DAYS);
            }
        };
    }


    /**
     * 快捷创建附带计数器容器的{@link SimpleBufferTriggerBuilder}实例.
     * <p>
     * 目前不推荐使用
     */
    public static SimpleBufferTriggerBuilder<Object, Map<Object, Integer>> newCounterBuilder() {
        return new SimpleBufferTriggerBuilder<Object, Map<Object, Integer>>()
                .setContainer(ConcurrentHashMap::new, (map, element) -> {
                    map.merge(element, 1, Math::addExact);
                    return true;
                });
    }

    /**
     * 将需要定时处理的元素推入缓存.
     *
     * <p>存在缓存容量检测，如果缓存已满，
     * 且拒绝回调方法已通过{@link SimpleBufferTriggerBuilder#rejectHandler(Consumer)}设置，
     * 则会回调该方法.
     *
     * <p>需要特别注意，该实现为了避免加锁引起的性能损耗，以及考虑到应用场景，缓存容量检测不进行严格计数，
     * 在并发场景下缓存中实际元素数量会略微超过预设的最大缓存容量；
     * 如需严格计数场景，请自行实现{@link BufferTrigger}.
     *
     * @param element 符合声明参数类型的元素
     */
    @Override
    public void enqueue(E element) {
        // 操作需要检测关闭标记
        checkState(!shutdown, "buffer trigger was shutdown.");

        long currentCount = counter.get();
        long maxBufferCount = this.maxCountSupplier.getAsLong();

        // 若超过阈值则看是否注册了拒绝策略
        if (MoreNumber.isPositive(maxBufferCount) && currentCount >= maxBufferCount) {
            boolean pass = true;
            if (rejectHandler != null) {
                // 不确定业务方设置的拒绝策略会执行何种操作，因此这里直接使用写锁
                if (writeLock != null && writeCondition != null) {
                    writeLock.lock();
                }
                try {
                    // 双重检查
                    // NOTE: 这里采用 DCL，是为了避免部分消费情况下没有 signalAll 唤醒，导致的卡死问题
                    currentCount = counter.get();
                    maxBufferCount = this.maxCountSupplier.getAsLong();
                    if (MoreNumber.isPositive(maxBufferCount) && currentCount >= maxBufferCount) {
                        pass = fireRejectHandler(element);
                    }
                } finally {
                    if (writeLock != null && writeCondition != null) {
                        writeLock.unlock();
                    }
                }
            }
            if (!pass) {
                return;
            }
        }

        // 拒绝策略是否放行意味着此处新的element是否需要

        // 需要读取buffer进行入队，因此需要读锁
        boolean locked = false;
        if (readLock != null) {
            try {
                readLock.lock();
                locked = true;
            } catch (Throwable e) {
                // ignore lock failed
            }
        }
        try {
            C thisBuffer = buffer.get();
            int changedCount = queueAdder.applyAsInt(thisBuffer, element);
            if (changedCount > 0) {
                counter.addAndGet(changedCount);
            }
        } finally {
            if (locked) {
                readLock.unlock();
            }
        }
    }

    private boolean fireRejectHandler(E element) {
        try {
            return rejectHandler.onReject(element, writeCondition);
        } catch (Throwable e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 直接使用了synchronized同步
     */
    @Override
    public void manuallyDoTrigger() {
        synchronized (SimpleBufferTrigger.this) {
            doConsume();
        }
    }

    /**
     * 目前所有触发消费的地方都有同步锁
     * NOTE: 有设置读写锁时，同步锁看来是没有必要的
     */
    private void doConsume() {
        C old = null;

        try {
            if (writeLock != null) {
                writeLock.lock();
            }
            try {
                old = buffer.getAndSet(bufferFactory.get());
            } finally {
                counter.set(0);
                if (writeCondition != null) {
                    writeCondition.signalAll();
                }

                if (writeLock != null) {
                    writeLock.unlock();
                }
            }

            if (old != null) {
                consumer.accept(old);
            }
        } catch (Throwable e) {
            if (this.exceptionHandler != null) {
                try {
                    this.exceptionHandler.accept(e, old);
                } catch (Throwable idontcare) {
                    e.printStackTrace();
                    idontcare.printStackTrace();
                }
            } else {
                log.error("Ops.", e);
            }
        }
    }

    @Override
    public long getPendingChanges() {
        return counter.get();
    }

    /**
     * 停止该实例，执行线程池shutdown操作.
     * <p>
     * 在停止实例前，会强制触发一次消费，用于清空缓存中尚未消费的元素.
     * <p>
     * 如未通过{@link GenericSimpleBufferTriggerBuilder#setScheduleExecutorService(ScheduledExecutorService)}
     * 自定义计划任务线程池，会调用默认计划任务线程池shutdown()方法，如未停止成功，
     * 会保留1天的超时时间再强制停止线程池.
     */
    @Override
    public void close() {
        shutdown = true;
        try {
            manuallyDoTrigger();
        } finally {
            shutdownExecutor.run();
        }
    }



    @AllArgsConstructor
    public class TriggerRunnable implements Runnable {

        // 触发所用定时任务线程池
        private final ScheduledExecutorService scheduledExecutorService;
        // 触发策略
        private final TriggerStrategy triggerStrategy;
        // 外层指针
        private final SimpleBufferTrigger that;


        @Override
        public void run() {
            synchronized (that) {
                long nextTrigPeriod = DEFAULT_NEXT_TRIGGER_PERIOD;

                try {
                    // 调用触发策略获取是否触发，以及下一次触发时间
                    TriggerResult triggerResult =
                            triggerStrategy.canTrigger(that.lastConsumeTimestamp, counter.get());
                    long beforeConsume = currentTimeMillis();
                    if (triggerResult.isDoConsume()) {
                        that.lastConsumeTimestamp = beforeConsume;
                        that.doConsume();
                    }

                    // 下一次触发间隔包含当前的执行时间
                    nextTrigPeriod = triggerResult.getNextPeriod();
                    nextTrigPeriod = nextTrigPeriod - (currentTimeMillis() - beforeConsume);
                } catch (Throwable e) {
                    log.error("", e);
                }

                // 确保下一次触发间隔大于0
                nextTrigPeriod = Math.max(0, nextTrigPeriod);
                // 如果未关闭，则设定下一个定时任务
                if (!that.shutdown) {
                    this.scheduledExecutorService.schedule(this, nextTrigPeriod, MILLISECONDS);
                }
            }
        }
    }


    public static void setupGlobalBackPressure(GlobalBackPressureListener listener) {
        BackPressureHandler.setupGlobalBackPressureListener(listener);
    }
    // 设置全局名称注册中心
    public static void setupGlobalNameRegistry(NameRegistry nameRegistry) {
        SimpleBufferTriggerBuilder.setupGlobalNameRegistry(nameRegistry);
    }

    /**
     * 该方法即将废弃，可更换为{@link MoreBufferTrigger#simple()}创建适合大多数场景的通用实例.
     */
    @Deprecated
    public static SimpleBufferTriggerBuilder<Object, Object> newBuilder() {
        return new SimpleBufferTriggerBuilder<>();
    }

    /**
     * 该方法即将废弃，可更换为{@link MoreBufferTrigger#simple()}创建适合大多数场景的通用实例.
     */
    @Deprecated
    public static <E, C> GenericSimpleBufferTriggerBuilder<E, C> newGenericBuilder() {
        return new GenericSimpleBufferTriggerBuilder<>(newBuilder());
    }
}
