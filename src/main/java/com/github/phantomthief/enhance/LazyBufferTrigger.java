package com.github.phantomthief.enhance;

import static com.github.phantomthief.util.MoreSuppliers.lazy;

import java.util.function.Supplier;

import com.github.phantomthief.BufferTrigger;
import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;

/**
 * 并非提供BufferTrigger的实现，而是对其的一个lazy封装
 * @author w.vela
 */
public class LazyBufferTrigger<E> implements BufferTrigger<E> {

    private final CloseableSupplier<BufferTrigger<E>> factory;

    public LazyBufferTrigger(Supplier<BufferTrigger<E>> factory) {
        this.factory = lazy(factory);
    }

    @Override
    public void enqueue(E element) {
        this.factory.get().enqueue(element);
    }

    @Override
    public void manuallyDoTrigger() {
        this.factory.ifPresent(BufferTrigger::manuallyDoTrigger);
    }

    @Override
    public long getPendingChanges() {
        return this.factory.map(BufferTrigger::getPendingChanges).orElse(0L);
    }

    @Override
    public void close() {
        this.factory.ifPresent(BufferTrigger::close);
    }
}
