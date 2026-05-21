package com.bnpparibasfortis.marvin.concurrency;

import java.util.*;
import java.util.concurrent.StructuredTaskScope.Subtask;

/** Aggregated outcome of {@link StructuredCallExecutor.Scope#execute()}. */
public final class StructuredResult {

    private final Map<String, Subtask<?>> subtasks;

    StructuredResult(Map<String, Subtask<?>> subtasks) {
        this.subtasks = Map.copyOf(subtasks);
    }

    /**
     * The successful value of the given fork.
     * @throws NoSuchElementException if the key is unknown
     * @throws IllegalStateException if the fork did not succeed
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        Subtask<?> s = require(key);
        if (s.state() != Subtask.State.SUCCESS) {
            throw new IllegalStateException(
                "Fork '" + key + "' state=" + s.state() +
                (s.state() == Subtask.State.FAILED ? " cause=" + s.exception() : ""));
        }
        return (T) s.get();
    }

    public boolean succeeded(String key) {
        return require(key).state() == Subtask.State.SUCCESS;
    }

    public Optional<Throwable> failure(String key) {
        Subtask<?> s = require(key);
        return s.state() == Subtask.State.FAILED
                ? Optional.of(s.exception())
                : Optional.empty();
    }

    public Set<String> keys() {
        return subtasks.keySet();
    }

    private Subtask<?> require(String key) {
        Subtask<?> s = subtasks.get(key);
        if (s == null) throw new NoSuchElementException("No fork with key: " + key);
        return s;
    }
}