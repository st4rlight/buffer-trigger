package com.github.phantomthief.backpressure;

/**
 * @author w.vela
 * Created on 2020-06-08.
 */
public interface BackPressureListener<T> {

    /**
     * 因为背压占用了拒绝策略，因此这里提供一个钩子给业务方
     */
    void onHandle(T element);
}
