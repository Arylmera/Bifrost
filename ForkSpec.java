package com.bnpparibasfortis.marvin.concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

final class ForkSpec<T> {

    private final String key;
    private final Callable<T> task;
    private final List<HandlerEntry<T>> handlers = new ArrayList<>();

    ForkSpec(String key, Callable<T> task) {
        this.key = key;
        this.task = task;
    }

    String key() { return key; }
    Callable<T> task() { return task; }

    void addHandler(Class<? extends Throwable> type, FailureHandler<T> handler) {
        handlers.add(new HandlerEntry<>(type, handler));
    }

    /** Returns the first registered handler whose type matches, or {@code null}. */
    FailureHandler<T> findHandler(Throwable ex) {
        for (HandlerEntry<T> entry : handlers) {
            if (entry.type().isInstance(ex)) return entry.handler();
        }
        return null;
    }

    private record HandlerEntry<T>(Class<? extends Throwable> type, FailureHandler<T> handler) {}
}