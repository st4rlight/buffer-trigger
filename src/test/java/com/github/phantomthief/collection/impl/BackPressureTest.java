package com.github.phantomthief.collection.impl;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.MoreBufferTrigger;
import com.github.phantomthief.BufferTrigger;
import com.github.phantomthief.backpressure.GlobalBackPressureListener;
import com.github.phantomthief.simple.SimpleBufferTrigger;
import com.google.common.util.concurrent.AtomicLongMap;

/**
 * @author w.vela
 * Created on 2019-07-30.
 */
class BackPressureTest {

    private static final Logger logger = LoggerFactory.getLogger(BackPressureTest.class);

    @Test
    void test() {
        List<String> consumed = new ArrayList<>();
        List<String> backPressured = Collections.synchronizedList(new ArrayList<>());
        BufferTrigger<String> buffer = MoreBufferTrigger.<String, List<String>> simple()
                .enableBackPressure(backPressured::add)
                .maxBufferCount(10)
                .interval(1, SECONDS)
                .setContainer(() -> synchronizedList(new ArrayList<>()), List::add)
                .consumer(it -> {
                    logger.info("do consuming...{}", it);
                    sleepUninterruptibly(1, SECONDS);
                    consumed.addAll(it);
                    logger.info("consumer done.{}", it);
                })
                .build();
        long cost = System.currentTimeMillis();
        ExecutorService executor = newFixedThreadPool(10);
        for (int i = 0; i < 30; i++) {
            int j = i;
            executor.execute(() -> {
                buffer.enqueue("" + j);
                logger.info("enqueued:{}", j);
            });
        }
        shutdownAndAwaitTermination(executor, 1, DAYS);
        assertTrue(backPressured.size() > 10);
        buffer.manuallyDoTrigger();
        assertEquals(30, consumed.size());
        cost = System.currentTimeMillis() - cost;
        assertTrue(cost >= SECONDS.toMillis(3));
    }

    @Test
    void testNoBlock() {
        List<String> consumed = new ArrayList<>();
        BufferTrigger<String> buffer = MoreBufferTrigger.<String, List<String>> simple()
                .maxBufferCount(10)
                .interval(1, SECONDS)
                .setContainer(() -> synchronizedList(new ArrayList<>()), List::add)
                .consumer(it -> {
                    logger.info("do consuming...{}", it);
                    sleepUninterruptibly(1, SECONDS);
                    consumed.addAll(it);
                    logger.info("consumer done.{}", it);
                })
                .build();
        long cost = System.currentTimeMillis();
        ExecutorService executor = newFixedThreadPool(10);
        for (int i = 0; i < 30; i++) {
            int j = i;
            executor.execute(() -> {
                buffer.enqueue("" + j);
                logger.info("enqueued:{}", j);
            });
        }
        shutdownAndAwaitTermination(executor, 1, DAYS);
        buffer.manuallyDoTrigger();
        cost = System.currentTimeMillis() - cost;
        assertTrue(cost <= 1200);
    }

    @Test
    void testGlobalHandler() {
        AtomicLongMap<String> onHandle = AtomicLongMap.create();
        AtomicLongMap<String> postHandle = AtomicLongMap.create();
        SimpleBufferTrigger.setupGlobalBackPressure(new GlobalBackPressureListener() {
            @Override
            public void onHandle(@Nullable String name, Object element) {
                onHandle.incrementAndGet(name);
            }

            @Override
            public void postHandle(@Nullable String name, Object element, long blockInNano) {
                postHandle.addAndGet(name, blockInNano);
            }
        });
        List<String> consumed = new ArrayList<>();
        List<String> backPressured = Collections.synchronizedList(new ArrayList<>());
        String name = "test-1";
        BufferTrigger<String> buffer = MoreBufferTrigger.<String, List<String>> simple()
                .maxBufferCount(10)
                .enableBackPressure(backPressured::add)
                .interval(1, SECONDS)
                .setContainer(() -> synchronizedList(new ArrayList<>()), List::add)
                .consumer(it -> {
                    logger.info("do consuming...{}", it);
                    sleepUninterruptibly(1, SECONDS);
                    consumed.addAll(it);
                    logger.info("consumer done.{}", it);
                })
                .name(name)
                .build();
        long cost = System.currentTimeMillis();
        ExecutorService executor = newFixedThreadPool(10);
        for (int i = 0; i < 30; i++) {
            int j = i;
            executor.execute(() -> {
                buffer.enqueue("" + j);
                logger.info("enqueued:{}", j);
            });
        }
        shutdownAndAwaitTermination(executor, 1, DAYS);
        assertTrue(backPressured.size() > 10);
        assertTrue(onHandle.get(name) > 10);
        buffer.manuallyDoTrigger();
        assertEquals(30, consumed.size());
        cost = System.currentTimeMillis() - cost;
        assertTrue(cost >= SECONDS.toMillis(3));
        assertTrue(postHandle.get(name) >= SECONDS.toNanos(3));
    }
}
