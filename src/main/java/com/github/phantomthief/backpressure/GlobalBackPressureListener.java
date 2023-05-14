package com.github.phantomthief.backpressure;

import javax.annotation.Nullable;

/**
 * @author w.vela
 * Created on 2021-02-04.
 */
public interface GlobalBackPressureListener {

    /**
     * @param name 业务名
     * @param element 当前元素
     */
    void onHandle(@Nullable String name, Object element);

    /**
     * @param name 业务名
     * @param element 当前元素
     * @param blockInNano 本次背压阻塞时间
     */
    void postHandle(@Nullable String name, Object element, long blockInNano);
}
