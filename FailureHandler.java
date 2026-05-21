package com.bnpparibasfortis.marvin.concurrency;

/**
 * Handles a {@link Throwable} raised by a forked task.
 *
 * <p>The handler may either:
 * <ul>
 *   <li><b>Rescue</b> — return a fallback value of type {@code T}. The fork
 *       then appears successful to the joiner.</li>
 *   <li><b>Escalate</b> — throw a domain exception (typically translated
 *       from the original cause). Under {@link JoinPolicy#ALL_SUCCESSFUL_OR_THROW}
 *       this trips the joiner and the domain exception bubbles out.</li>
 * </ul>
 */
@FunctionalInterface
public interface FailureHandler<T> {
    T handle(Throwable ex) throws Exception;
}