package com.github.phantomthief.simple;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.newSetFromMap;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import javax.annotation.Nonnull;

import com.github.phantomthief.BufferTrigger;
import com.github.phantomthief.backpressure.BackPressureHandler;
import com.github.phantomthief.backpressure.BackPressureListener;
import com.github.phantomthief.enhance.LazyBufferTrigger;
import com.github.phantomthief.strategy.MultiIntervalTriggerStrategy;
import com.github.phantomthief.strategy.TriggerResult;
import com.github.phantomthief.strategy.TriggerStrategy;
import com.github.phantomthief.support.NameRegistry;
import com.github.phantomthief.support.RejectHandler;
import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link SimpleBufferTrigger}构造器，目前已不推荐直接使用，请调用{@link com.github.phantomthief.MoreBufferTrigger#simple()}生成构造器.
 * <p>
 * 用于标准化{@link SimpleBufferTrigger}实例生成及配置，提供部分机制的默认实现
 * @param <E> 缓存元素类型，标明{@link  SimpleBufferTrigger#enqueue(Object)}传入元素的类型
 * @param <C> 缓存容器类型
 */
@Slf4j
@SuppressWarnings("unchecked")
public class SimpleBufferTriggerBuilder<E, C> {

    private static NameRegistry globalNameRegistry;
    // 业务名称（仅当使用内部实现线程池时生效，用于线程名字中）
    String bizName;
    // 是否关闭读写锁，默认不关，关了可能丢数据
    boolean disableSwitchLock;

    // 触发策略
    TriggerStrategy triggerStrategy;
    // 定时任务线程池（若使用自定义线程池要注意调用close方法后手动shutdown和awaitTermination）
    // 默认实现是一个单个线程的线程池，且是daemon线程
    ScheduledExecutorService scheduledExecutorService;
    // 标识是否使用了默认内部实现线程池
    boolean usingInnerExecutor;

    // 提供容器的工厂（需要考虑enqueue时的并发问题，因此建议使用线程安全的容器）
    Supplier<C> bufferFactory;
    // 入队方法，需要返回本次新增成功个数
    ToIntBiFunction<C, E> queueAdder;
    // 入队拒绝策略
    RejectHandler<E> rejectHandler;
    // 最大容量限制
    LongSupplier maxBufferCount = () -> -1;
    private boolean maxBufferCountWasSet = false;

    // 具体的消费函数（需要避免长时间无法消费完成）
    ThrowableConsumer<C, Throwable> consumer;
    // 消费异常触发方法
    BiConsumer<Throwable, C> exceptionHandler;


    /**
     * 设置缓存提供器及缓存存入函数；<b>注意：</b> 请使用者必须考虑线程安全问题.
     * <p>
     * 为了保证不同场景下的最优性能，框架本身不保证{@link BufferTrigger#enqueue(Object)}操作的线程安全，
     * 一般该函数调用位于处理请求线程池，该函数的耗时将直接影响业务服务并发效率，
     * 推荐使用者实现更贴合实际业务的高性能线程安全容器.
     * <p>
     * 另，如确定{@link BufferTrigger#enqueue(Object)}为单线程调用，无需使用线程安全容器.
     * <p>
     * 推荐使用{@link #setContainerEx(Supplier, ToIntBiFunction)}，
     * 对于需要传入的queueAdder类型为{@link ToIntBiFunction}，更符合计数行为需求.
     * @param factory 缓存提供器；要求为{@link Supplier}，因为执行消费回调后，缓存会被清空，需要执行提供器再次获得可用缓存对象
     * @param queueAdder 缓存更新方法，要求返回元素插入结果（布尔值），如缓存容器实现为{@link java.util.HashSet},
     * 在插入相同元素时会返回false，类库需要获得该结果转化为缓存中变动元素个数并计数，该计数可能作为消费行为的触发条件.
     * @param <E1> 缓存元素类型
     * @param <C1> 缓存容器类型
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> setContainer(Supplier<? extends C1> factory,
            BiPredicate<? super C1, ? super E1> queueAdder) {
        checkNotNull(factory);
        checkNotNull(queueAdder);

        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.bufferFactory = (Supplier<C1>) factory;
        thisBuilder.queueAdder = (c, e) -> queueAdder.test(c, e) ? 1 : 0;
        return thisBuilder;
    }

    /**
     * 设置缓存提供器及缓存存入函数；<b>注意：</b> 必须使用线程安全容器.
     * <p>
     * 为了保证不同场景下的最优性能，框架本身不保证{@link BufferTrigger#enqueue(Object)}操作的线程安全，
     * 一般函数调用位于处理请求线程池，该函数的耗时将直接影响业务服务并发效率，
     * 推荐使用者实现更贴合实际业务的线程安全容器.
     * <p>
     * 另，如确定{@link BufferTrigger#enqueue(Object)}为单线程调用，无需使用线程安全容器.
     * @param factory 缓存提供器；要求为{@link Supplier}，因为执行消费回调后，缓存会被清空，需要执行提供器再次获得可用缓存对象
     * @param queueAdder 缓存更新方法，要求返回元素变动个数，类库需要将结果计数，该计数可能作为消费行为的触发条件.
     * @param <E1> 缓存元素类型
     * @param <C1> 缓存容器类型
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> setContainerEx(
            Supplier<? extends C1> factory, ToIntBiFunction<? super C1, ? super E1> queueAdder) {
        checkNotNull(factory);
        checkNotNull(queueAdder);

        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.bufferFactory = (Supplier<C1>) factory;
        thisBuilder.queueAdder = (ToIntBiFunction<C1, E1>) queueAdder;
        return thisBuilder;
    }

    /**
     * 自定义计划任务线程池，推荐使用内部默认实现.
     * <p>
     * 如使用自定义线程池，在调用{@link BufferTrigger#close()}方法后需要手动调用{@link ScheduledExecutorService#shutdown()}
     * 以及{@link ScheduledExecutorService#awaitTermination(long, TimeUnit)}方法来保证线程池平滑停止.
     */
    public SimpleBufferTriggerBuilder<E, C> setScheduleExecutorService(
            ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        return this;
    }

    /**
     * 设置异常处理器.
     * <p>
     * 该处理器会在消费异常时执行.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1>
            setExceptionHandler(BiConsumer<? super Throwable, ? super C1> exceptionHandler) {
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.exceptionHandler = (BiConsumer<Throwable, C1>) exceptionHandler;
        return thisBuilder;
    }

    /**
     * 是否关闭读写锁，默认读写锁为开启状态，不推荐关闭.
     * <p>
     * {@link BufferTrigger}使用场景可归纳为读多写少，{@link SimpleBufferTrigger}的实现中默认使用了
     * 可重入读写锁{@link java.util.concurrent.locks.ReentrantReadWriteLock}
     */
    public SimpleBufferTriggerBuilder<E, C> disableSwitchLock() {
        this.disableSwitchLock = true;
        return this;
    }

    /**
     * 设置消费触发策略.
     * @param triggerStrategy {@link TriggerStrategy}具体实现，
     * 推荐传入{@link MultiIntervalTriggerStrategy}实例
     */
    public SimpleBufferTriggerBuilder<E, C> triggerStrategy(TriggerStrategy triggerStrategy) {
        this.triggerStrategy = triggerStrategy;
        return this;
    }

    /**
     * 该方法即将废弃，请使用{@link #interval(long, TimeUnit)}或{@link #triggerStrategy}设置消费策略.
     */
    @Deprecated
    public SimpleBufferTriggerBuilder<E, C> on(long interval, TimeUnit unit, long count) {
        if (this.triggerStrategy == null) {
            this.triggerStrategy = new MultiIntervalTriggerStrategy();
        }
        if (this.triggerStrategy instanceof MultiIntervalTriggerStrategy) {
            ((MultiIntervalTriggerStrategy) this.triggerStrategy).on(interval, unit, count);
        } else {
            log.warn(
                    "exists non multi interval trigger strategy found. ignore setting:{},{}->{}",
                    interval, unit, count);
        }
        return this;
    }

    /**
     * 设置定期触发的简单消费策略，{@link #interval(LongSupplier, TimeUnit)}易用封装
     * <p>
     * 该方法间隔时间在初始化时设定，无法更改，如有动态间隔时间需求，请使用{@link #interval(LongSupplier, TimeUnit)}
     * @param interval 间隔时间
     * @param unit 间隔时间单位
     */
    public SimpleBufferTriggerBuilder<E, C> interval(long interval, TimeUnit unit) {
        return this.interval(() -> interval, unit);
    }

    /**
     * 设置定期触发的简单消费策略.
     * <p>
     * 该方法提供了动态更新时间间隔的需求，如时间间隔设置在配置中心或redis中，会因场景动态变化.
     * <p>
     * 如果需同时存在多个不同周期的消费策略，请使用{@link MultiIntervalTriggerStrategy}
     * @param interval 间隔时间提供器，可自行实现动态时间
     * @param unit 间隔时间单位
     */
    public SimpleBufferTriggerBuilder<E, C> interval(LongSupplier interval, TimeUnit unit) {
        this.triggerStrategy = (last, change) -> {
            long intervalInMs = unit.toMillis(interval.getAsLong());
            // 默认实现只要在超过时间，且容器里有东西，就触发
            return TriggerResult.of(change > 0 && currentTimeMillis() - last >= intervalInMs, intervalInMs);
        };
        return this;
    }

    /**
     * 设置消费回调函数.
     * <p>
     * 该方法用于设定消费行为，由使用者自行提供消费回调函数，需要注意，
     * 回调注入的对象为当前消费时间点，缓存容器中尚存的所有元素，非逐个元素消费.
     * @param consumer 
     * @param <E1> 缓存元素类型
     * @param <C1> 缓存容器类型
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1>
            consumer(ThrowableConsumer<? super C1, Throwable> consumer) {
        checkNotNull(consumer);
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.consumer = (ThrowableConsumer<C1, Throwable>) consumer;
        return thisBuilder;
    }

    /**
     * 设置缓存最大容量（数量）.
     * <p>
     * 推荐由缓存容器实现该限制
     */
    public SimpleBufferTriggerBuilder<E, C> maxBufferCount(long count) {
        checkArgument(count > 0);
        return maxBufferCount(() -> count);
    }

    /**
     * 设置缓存最大容量（数量）提供器，适合动态场景.
     * <p>
     * 推荐由缓存容器实现该限制
     */
    public SimpleBufferTriggerBuilder<E, C> maxBufferCount(@Nonnull LongSupplier count) {
        this.maxBufferCount = checkNotNull(count);
        maxBufferCountWasSet = true;
        return this;
    }

    /**
     * 同时设置缓存最大容量（数量）以及拒绝推入处理器
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> maxBufferCount(long count,
            Consumer<? super E1> rejectHandler) {
        return (SimpleBufferTriggerBuilder<E1, C1>) maxBufferCount(count)
                .rejectHandler(rejectHandler);
    }

    /**
     * 设置拒绝推入缓存处理器.
     * <p>
     * 当设置maxBufferCount，且达到上限时回调该处理器；推荐由缓存容器实现该限制.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> rejectHandler(Consumer<? super E1> rejectHandler) {
        checkNotNull(rejectHandler);
        return this.rejectHandlerEx((e, h) -> {
            rejectHandler.accept(e);
            return false;
        });
    }

    /**
     * it's better dealing this in container
     * NOTE: 若使用背压则无法设置拒绝策略
     */
    private <E1, C1> SimpleBufferTriggerBuilder<E1, C1> rejectHandlerEx(RejectHandler<? super E1> rejectHandler) {
        checkNotNull(rejectHandler);
        if (this.rejectHandler instanceof BackPressureHandler) {
            throw new IllegalStateException("cannot set reject handler while enable back-pressure.");
        }
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.rejectHandler = (RejectHandler<E1>) rejectHandler;
        return thisBuilder;
    }

    /**
     * 开启背压(back-pressure)能力，无处理回调.
     * <p>
     * <b>注意，当开启背压时，需要配合 {@link #maxBufferCount(long)}
     * 并且不要设置 {@link #rejectHandler}.</b>
     * <p>
     * 当buffer达到最大值时，会阻塞{@link BufferTrigger#enqueue(Object)}调用，直到消费完当前buffer后再继续执行；
     * 由于enqueue大多处于处理请求线程池中，如开启背压，大概率会造成请求线程池耗尽，此类场景建议直接丢弃入队元素或转发至Kafka.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> enableBackPressure() {
        return enableBackPressure(null);
    }

    /**
     * 开启背压(back-pressure)能力，接收处理回调方法.
     * <p>
     * <b>注意，当开启背压时，需要配合 {@link #maxBufferCount(long)}
     * 并且不要设置 {@link #rejectHandler}.</b>
     * <p>
     * 当buffer达到最大值时，会阻塞{@link BufferTrigger#enqueue(Object)}调用，直到消费完当前buffer后再继续执行；
     * 由于enqueue大多处于处理请求线程池中，如开启背压，大概率会造成请求线程池耗尽，此类场景建议直接丢弃入队元素或转发至Kafka.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> enableBackPressure(BackPressureListener<E1> listener) {
        // 背压是通过拒绝回调实现的，当前只支持一个拒绝策略
        // 因此不允许有自定义的拒绝策略
        if (this.rejectHandler != null) {
            throw new IllegalStateException("cannot enable back-pressure while reject handler was set.");
        }
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.rejectHandler = new BackPressureHandler<>(listener);
        return thisBuilder;
    }

    /**
     * 设置计划任务线程名称.
     * <p>
     * 仅当使用默认计划任务线程池时生效，线程最终名称为pool-simple-buffer-trigger-thread-[name].
     */
    public SimpleBufferTriggerBuilder<E, C> name(String name) {
        this.bizName = name;
        return this;
    }

    /**
     * 生成实例
     */
    public <E1> BufferTrigger<E1> build() {
        // 背压校验
        check();

        // 获取到build函数被调用的位置
        if (globalNameRegistry != null && bizName == null) {
            bizName = globalNameRegistry.name();
        }
        return new LazyBufferTrigger<>(() -> {
            // ensure主要是提供一些默认参数实现
            ensure();
            // supplier实现
            SimpleBufferTriggerBuilder<E1, C> builder =
                    (SimpleBufferTriggerBuilder<E1, C>) SimpleBufferTriggerBuilder.this;
            return new SimpleBufferTrigger<>(builder);
        });
    }

    /**
     * 目前主要是开启背压时
     * 1. 不能禁用读写锁
     * 2. 必须设置最大buffer容量
     * 3. 不能设置拒绝策略
     */
    private void check() {
        checkNotNull(consumer);
        if (rejectHandler instanceof BackPressureHandler) {
            if (disableSwitchLock) {
                throw new IllegalStateException("back-pressure cannot work together with switch lock disabled.");
            }
            if (!maxBufferCountWasSet) {
                throw new IllegalStateException("back-pressure need to set maxBufferCount.");
            }
        }
    }

    private void ensure() {
        if (this.triggerStrategy == null) {
            log.warn("no trigger strategy found. using NO-OP trigger");
            this.triggerStrategy = (t, n) -> TriggerResult.empty();
        }

        if (this.bufferFactory == null && this.queueAdder == null) {
            log.warn("no container found. use default thread-safe HashSet as container.");
            this.bufferFactory = () -> (C) newSetFromMap(new ConcurrentHashMap<>());
            this.queueAdder = (c, e) -> ((Set<E>) c).add(e) ? 1 : 0;
        }
        if (this.scheduledExecutorService == null) {
            this.scheduledExecutorService = this.makeScheduleExecutor();
            this.usingInnerExecutor = true;
        }
        if (this.bizName != null && rejectHandler instanceof BackPressureHandler) {
            // 给背压处理器设置bizName，主要被用于背压钩子函数中
            ((BackPressureHandler<E>) rejectHandler).setName(bizName);
        }
    }

    private ScheduledExecutorService makeScheduleExecutor() {
        String threadPattern = bizName == null
               ? "pool-simple-buffer-trigger-thread-%d"
               : "pool-simple-buffer-trigger-thread-[" + bizName + "]";

        // 默认只有单个线程
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(threadPattern)
                .setDaemon(true)
                .build();
        return Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    /**
     * 目前名称主要是用于获取buffer-trigger被声明的位置
     * NOTE: 通过simple()获取builder后build，在build函数中会对全局命名注册进行name调用
     */
    static void setupGlobalNameRegistry(NameRegistry registry) {
        globalNameRegistry = registry;
    }
}