package com.bnpparibasfortis.marvin.concurrency;

import io.opentelemetry.context.Context;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Marvin's facade over Java 25 {@link StructuredTaskScope} (JEP 505).
 *
 * <p>Removes the boilerplate for the typical "fan-out to thin clients,
 * aggregate results" pattern while keeping the structured nature of the
 * underlying API visible at the call site.
 *
 * <h2>Defaults applied when not specified</h2>
 * <table>
 *   <tr><th>Setting</th><th>Marvin default</th><th>Reason</th></tr>
 *   <tr>
 *     <td>{@link JoinPolicy Joiner}</td>
 *     <td>{@link JoinPolicy#ALL_SUCCESSFUL_OR_THROW}</td>
 *     <td>Matches the typical thin-client orchestration pattern.
 *         Java's no-arg {@code StructuredTaskScope.open()} uses
 *         {@code awaitAll()}, which silently swallows failures —
 *         too lenient for a banking context.</td>
 *   </tr>
 *   <tr>
 *     <td>Timeout</td>
 *     <td>10 seconds (see {@link #DEFAULT_TIMEOUT})</td>
 *     <td>Java's default is unbounded. A missing timeout is
 *         almost always a bug here; override per call when 10s
 *         is wrong.</td>
 *   </tr>
 *   <tr>
 *     <td>Thread factory</td>
 *     <td>Virtual threads (Java default)</td>
 *     <td>Not overridable. Marvin guarantees virtual threads
 *         so that context propagation is consistent.</td>
 *   </tr>
 *   <tr>
 *     <td>Scope name</td>
 *     <td>{@code "marvin-scope-N"} (monotonic)</td>
 *     <td>Visible in thread dumps. Override via
 *         {@link Scope#name(String)} for clearer diagnostics.</td>
 *   </tr>
 *   <tr>
 *     <td>Per-fork failure handler</td>
 *     <td>None</td>
 *     <td>Exception propagates and (under
 *         {@code ALL_SUCCESSFUL_OR_THROW}) trips the joiner —
 *         identical to raw Java behaviour. Override with
 *         {@link Scope#onFailure}.</td>
 *   </tr>
 * </table>
 *
 * <h2>Context propagation</h2>
 * <ul>
 *   <li><b>MDC</b>: copied into each virtual thread automatically.</li>
 *   <li><b>OpenTelemetry {@link Context}</b>: propagated automatically;
 *       spans opened inside a fork have the calling span as parent.</li>
 *   <li><b>{@link java.lang.ScopedValue}</b>: inherited natively by
 *       structured concurrency. Bind values before {@link Scope#execute()}.</li>
 *   <li><b>Plain {@code ThreadLocal}</b>: <em>not</em> propagated. Migrate to
 *       {@code ScopedValue}.</li>
 * </ul>
 *
 * <h2>Example</h2>
 * <pre>{@code
 * var result = executor.scope()
 *     .timeout(Duration.ofSeconds(2))
 *     .fork("user", () -> userClient.fetch(id))
 *         .onFailure(ex -> { throw new UserUnavailableException(id, ex); })
 *     .fork("orders", () -> orderClient.list(id))
 *         .onFailure(ex -> List.of())
 *     .fork("preferences", () -> prefsClient.fetch(id))
 *         .onFailure(IOException.class, ex -> Preferences.defaults())
 *     .execute();
 *
 * User user = result.get("user");
 * List<Order> orders = result.get("orders");
 * }</pre>
 */
@Component
public class StructuredCallExecutor {

    public static final JoinPolicy DEFAULT_POLICY = JoinPolicy.ALL_SUCCESSFUL_OR_THROW;
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    private static final AtomicLong SCOPE_COUNTER = new AtomicLong();

    /** Starts a new fluent scope builder. */
    public Scope scope() {
        return new Scope();
    }

    public class Scope {

        private JoinPolicy policy = DEFAULT_POLICY;
        private Duration timeout = DEFAULT_TIMEOUT;
        private String name = "marvin-scope-" + SCOPE_COUNTER.incrementAndGet();
        private final List<ForkSpec<?>> forks = new ArrayList<>();
        private ForkSpec<?> currentFork;

        public Scope policy(JoinPolicy policy) {
            this.policy = Objects.requireNonNull(policy);
            return this;
        }

        public Scope timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout);
            return this;
        }

        public Scope name(String name) {
            this.name = Objects.requireNonNull(name);
            return this;
        }

        public <T> Scope fork(String key, Callable<T> task) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(task, "task");
            if (forks.stream().anyMatch(f -> f.key().equals(key))) {
                throw new IllegalArgumentException("Duplicate fork key: " + key);
            }
            ForkSpec<T> spec = new ForkSpec<>(key, task);
            forks.add(spec);
            currentFork = spec;
            return this;
        }

        /** Catches every {@link Throwable} from the most recent {@link #fork}. */
        public <T> Scope onFailure(FailureHandler<T> handler) {
            return onFailure(Throwable.class, handler);
        }

        /** Catches only the given exception type from the most recent {@link #fork}. */
        @SuppressWarnings("unchecked")
        public <T, E extends Throwable> Scope onFailure(Class<E> type, FailureHandler<T> handler) {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(handler, "handler");
            if (currentFork == null) {
                throw new IllegalStateException("onFailure() must follow a fork()");
            }
            ((ForkSpec<T>) currentFork).addHandler(type, handler);
            return this;
        }

        public StructuredResult execute() throws Exception {
            if (forks.isEmpty()) {
                throw new IllegalStateException("At least one fork() is required");
            }

            // Snapshot caller context for propagation into virtual threads.
            Map<String, String> mdcSnapshot = MDC.getCopyOfContextMap();
            Context otelSnapshot = Context.current();

            Joiner<Object, Void> joiner = policy.joiner();

            try (var scope = StructuredTaskScope.open(joiner, cfg -> cfg
                    .withName(name)
                    .withTimeout(timeout))) {

                Map<String, Subtask<?>> subtasks = new LinkedHashMap<>(forks.size());
                for (ForkSpec<?> spec : forks) {
                    subtasks.put(spec.key(), scope.fork(wrap(spec, mdcSnapshot, otelSnapshot)));
                }

                try {
                    scope.join();
                } catch (StructuredTaskScope.FailedException ex) {
                    throw unwrap(ex);
                } catch (TimeoutException ex) {
                    throw new StructuredCallTimeoutException(name, timeout, ex);
                }
                return new StructuredResult(subtasks);
            }
        }

        private <T> Callable<T> wrap(ForkSpec<T> spec,
                                     Map<String, String> mdc,
                                     Context otel) {
            return () -> {
                Map<String, String> previousMdc = MDC.getCopyOfContextMap();
                if (mdc != null) MDC.setContextMap(mdc); else MDC.clear();
                try (var ignored = otel.makeCurrent()) {
                    try {
                        return spec.task().call();
                    } catch (Throwable ex) {
                        FailureHandler<T> handler = spec.findHandler(ex);
                        if (handler == null) throw ex;
                        return handler.handle(ex);
                    }
                } finally {
                    if (previousMdc != null) MDC.setContextMap(previousMdc); else MDC.clear();
                }
            };
        }

        private Exception unwrap(StructuredTaskScope.FailedException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException re) return re;
            if (cause instanceof Exception e) return e;
            if (cause instanceof Error err) throw err;
            return ex;
        }
    }
}