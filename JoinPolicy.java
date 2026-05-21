package com.bnpparibasfortis.marvin.concurrency;

import java.util.concurrent.StructuredTaskScope.Joiner;

/**
 * Completion policy for a structured scope.
 *
 * <p>Only the two joiners that fit a <em>keyed, heterogeneous</em> fork model
 * are exposed:
 * <ul>
 *   <li>{@link #ALL_SUCCESSFUL_OR_THROW} — fail-fast on first failure (default).</li>
 *   <li>{@link #AWAIT_ALL} — best-effort; inspect per-fork state on the result.</li>
 * </ul>
 *
 * <p>The other two JEP 505 joiners are intentionally not exposed here:
 * <ul>
 *   <li>{@code Joiner.allSuccessfulOrThrow()} returns a {@code Stream<Subtask<T>>} —
 *       requires a homogeneous return type, which conflicts with the keyed API.</li>
 *   <li>{@code Joiner.anySuccessfulResultOrThrow()} models a race, which is a
 *       different shape of problem and belongs on a separate method.</li>
 * </ul>
 */
public enum JoinPolicy {

    /**
     * Waits for all forks. The first failure cancels the remaining forks and
     * the exception propagates from {@code execute()} (unwrapped to the
     * underlying cause). Maps to {@link Joiner#awaitAllSuccessfulOrThrow()}.
     */
    ALL_SUCCESSFUL_OR_THROW {
        @Override public Joiner<Object, Void> joiner() {
            return Joiner.awaitAllSuccessfulOrThrow();
        }
    },

    /**
     * Waits for all forks regardless of outcome. Per-fork state is available
     * on the {@link StructuredResult}. Maps to {@link Joiner#awaitAll()}.
     */
    AWAIT_ALL {
        @Override public Joiner<Object, Void> joiner() {
            return Joiner.awaitAll();
        }
    };

    public abstract Joiner<Object, Void> joiner();
}