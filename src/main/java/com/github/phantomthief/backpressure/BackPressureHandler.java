package com.github.phantomthief.backpressure;

import static java.lang.System.nanoTime;

import java.util.concurrent.locks.Condition;

import javax.annotation.Nullable;

import com.github.phantomthief.support.RejectHandler;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author w.vela
 * Created on 2019-07-30.
 */
@Slf4j


public class BackPressureHandler<T> implements RejectHandler<T> {

    @Setter
    private String name;
    @Nullable
    private final BackPressureListener<T> listener;

    public BackPressureHandler(@Nullable BackPressureListener<T> listener) {
        this.listener = listener;
    }


    private static GlobalBackPressureListener globalBackPressureListener = null;

    public static void setupGlobalBackPressureListener(GlobalBackPressureListener listener) {
        globalBackPressureListener = listener;
    }



    @Override
    public boolean onReject(T element, @Nullable Condition condition) {
        // 自定义背压钩子
        if (listener != null) {
            try {
                listener.onHandle(element);
            } catch (Throwable e) {
                log.error("", e);
            }
        }

        // 全局背压钩子
        if (globalBackPressureListener != null) {
            try {
                globalBackPressureListener.onHandle(name, element);
            } catch (Throwable e) {
                log.error("", e);
            }
        }

        assert condition != null;
        long startNano = nanoTime();
        // 当前数据队列被处理后会signal，这里会阻塞新数据的插入
        condition.awaitUninterruptibly();
        long blockInNano = nanoTime() - startNano;

        // 背压全局后处理钩子
        if (globalBackPressureListener != null) {
            try {
                globalBackPressureListener.postHandle(name, element, blockInNano);
            } catch (Throwable e) {
                log.error("", e);
            }
        }

        return true;
    }
}
