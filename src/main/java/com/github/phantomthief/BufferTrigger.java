package com.github.phantomthief;

/**
 * 一个支持自定义消费策略的本地缓存.
 * <p>用于本地缓存指定条目数据集，定时进行批处理或聚合计算；可用于埋点聚合计算，点赞计数等不追求强一致的业务场景.
 * <p>大多数场景推荐调用{@link MoreBufferTrigger#simple()}来构造{@link BufferTrigger}.
 *
 * @author w.vela
 */
public interface BufferTrigger<E> extends AutoCloseable {

    /**
     * 将需要定时处理的元素推入缓存.
     *
     * @throws IllegalStateException 当实例被关闭时，调用该方法可能会引起该异常.
     */
    void enqueue(E element);

    /**
     * 手动触发一次缓存消费.
     * <p>一般处于缓存关闭方法{@link #close()}实现中.
     */
    void manuallyDoTrigger();

    /**
     * 获取当前缓存中未消费元素个数.
     */
    long getPendingChanges();

    /**
     * 停止该实例，释放所持有资源.
     * <p>
     * 请注意自动关闭特性仅在处于{@code try}-with-resources时才会生效，其它场景请在服务停止的回调方法中显式调用该方法.
     */
    @Override
    void close(); // override to remove throws Exception.
}
