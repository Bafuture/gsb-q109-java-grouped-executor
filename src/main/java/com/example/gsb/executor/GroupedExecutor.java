package com.example.gsb.executor;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executes tasks grouped by an arbitrary key.
 *
 * <ul>
 *   <li>Tasks submitted with the same key run strictly in submission order, one at a time.</li>
 *   <li>Tasks of different groups run in parallel, bounded by a configurable global
 *       parallelism; excess work waits in per-group queues.</li>
 *   <li>Groups that stay idle for a configurable timeout are reaped and transparently
 *       re-created when new work arrives.</li>
 *   <li>A throwing task never blocks its own group or other groups; failures are recorded
 *       and reported to a {@link TaskFailureHandler}.</li>
 *   <li>{@link #shutdown()} stops accepting new tasks; {@link #awaitTermination(Duration)}
 *       waits for already-accepted work to drain.</li>
 * </ul>
 *
 * <p>Thread-safe.
 */
public final class GroupedExecutor implements AutoCloseable {

    /** Snapshot of a recorded task failure plus the callback hook. */
    public static Builder builder() {
        return new Builder();
    }

    private final ThreadPoolExecutor pool;
    private final ScheduledExecutorService reaper;
    private final ConcurrentHashMap<Object, GroupState> groups = new ConcurrentHashMap<>();
    private final long idleTimeoutNanos;
    private final TaskFailureHandler failureHandler;
    private final List<TaskFailure> failures = new CopyOnWriteArrayList<>();

    /** Number of accepted tasks not yet completed; guarded by {@link #pendingMonitor}. */
    private final Object pendingMonitor = new Object();
    private int pending;

    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    private GroupedExecutor(Builder b) {
        this.idleTimeoutNanos = b.idleTimeout.toNanos();
        this.failureHandler = b.failureHandler;
        AtomicInteger seq = new AtomicInteger();
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "grouped-executor-worker-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.pool = new ThreadPoolExecutor(
                b.parallelism, b.parallelism,
                0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(),
                factory);
        this.pool.prestartAllCoreThreads();

        // Reaper: drops idle group entries so their bookkeeping memory is released.
        // Execution resources (worker threads) are shared and released automatically
        // whenever a group's drainer sees an empty queue.
        long periodMillis = Math.max(1L, b.idleTimeout.toMillis() / 2);
        this.reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "grouped-executor-reaper");
            t.setDaemon(true);
            return t;
        });
        this.reaper.scheduleWithFixedDelay(this::reapIdleGroups, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Submits a task for the given group key. Tasks of the same key execute strictly
     * in submission order.
     *
     * @throws RejectedExecutionException if the executor has been shut down
     */
    public void submit(Object groupKey, Runnable task) {
        Objects.requireNonNull(groupKey, "groupKey");
        Objects.requireNonNull(task, "task");
        if (!accepting.get()) {
            throw new RejectedExecutionException("GroupedExecutor is shut down");
        }
        synchronized (pendingMonitor) {
            if (!accepting.get()) {
                throw new RejectedExecutionException("GroupedExecutor is shut down");
            }
            pending++;
        }

        GroupState state = groups.computeIfAbsent(groupKey, k -> new GroupState());
        synchronized (state) {
            state.queue.addLast(task);
            if (!state.drainerScheduled) {
                state.drainerScheduled = true;
                try {
                    pool.execute(() -> drain(groupKey, state));
                } catch (RejectedExecutionException e) {
                    // Pool is gone (should not happen before shutdown completes); fail the task.
                    state.drainerScheduled = false;
                    state.queue.removeLast();
                    adjustPending(-1);
                    throw e;
                }
            }
        }
    }

    /** Runs queued tasks of one group sequentially until the queue is empty. */
    private void drain(Object key, GroupState state) {
        try {
            for (;;) {
                Runnable task;
                synchronized (state) {
                    task = state.queue.pollFirst();
                    if (task == null) {
                        state.drainerScheduled = false;
                        state.lastActiveNanos = System.nanoTime();
                        return;
                    }
                }
                try {
                    task.run();
                } catch (Throwable t) {
                    recordFailure(key, task, t);
                } finally {
                    adjustPending(-1);
                }
            }
        } catch (Throwable fatal) {
            // Never let a worker thread die silently; keep the group consistent.
            synchronized (state) {
                state.drainerScheduled = false;
            }
            throw fatal;
        }
    }

    private void recordFailure(Object key, Runnable task, Throwable t) {
        failures.add(new TaskFailure(key, task, t, Instant.now()));
        try {
            failureHandler.onFailure(key, task, t);
        } catch (Throwable handlerError) {
            // A broken handler must not kill the worker.
            handlerError.printStackTrace();
        }
    }

    private void adjustPending(int delta) {
        synchronized (pendingMonitor) {
            pending += delta;
            if (pending == 0) {
                pendingMonitor.notifyAll();
            }
        }
    }

    private void reapIdleGroups() {
        long now = System.nanoTime();
        for (var entry : groups.entrySet()) {
            GroupState state = entry.getValue();
            synchronized (state) {
                if (state.drainerScheduled || !state.queue.isEmpty()) {
                    continue;
                }
                if (now - state.lastActiveNanos < idleTimeoutNanos) {
                    continue;
                }
            }
            // Remove only if it is still the same (idle) instance; a racing submit
            // that re-created or reused the entry wins and keeps it alive.
            groups.remove(entry.getKey(), state);
        }
    }

    /** Current number of live (non-reaped) groups. Visible for testing/monitoring. */
    public int groupCount() {
        return groups.size();
    }

    /** All task failures recorded so far, in completion order. */
    public List<TaskFailure> failures() {
        return List.copyOf(failures);
    }

    /** Number of accepted tasks that have not finished yet (queued + running). */
    public int pendingCount() {
        synchronized (pendingMonitor) {
            return pending;
        }
    }

    /**
     * Stops accepting new tasks. Already-accepted tasks continue to be executed;
     * use {@link #awaitTermination(Duration)} to wait for them.
     */
    public void shutdown() {
        if (accepting.compareAndSet(true, false)) {
            reaper.shutdown();
            pool.shutdown();
        }
    }

    /**
     * Waits until all accepted tasks have completed or the timeout elapses.
     *
     * @return true if the executor drained completely, false on timeout
     */
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (pendingMonitor) {
            long remaining = timeout.toNanos();
            while (pending > 0 && remaining > 0) {
                long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
                int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
                pendingMonitor.wait(millis, nanos);
                remaining = deadline - System.nanoTime();
            }
            return pending == 0;
        }
    }

    /** Shuts down and waits indefinitely for all accepted tasks to finish. */
    @Override
    public void close() throws InterruptedException {
        shutdown();
        synchronized (pendingMonitor) {
            while (pending > 0) {
                pendingMonitor.wait();
            }
        }
        if (terminated.compareAndSet(false, true)) {
            pool.shutdownNow();
            reaper.shutdownNow();
        }
    }

    public static final class Builder {
        private int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
        private Duration idleTimeout = Duration.ofSeconds(30);
        private TaskFailureHandler failureHandler = (key, task, error) -> { };

        private Builder() {
        }

        /** Maximum number of tasks executing concurrently across all groups. */
        public Builder parallelism(int parallelism) {
            if (parallelism < 1) {
                throw new IllegalArgumentException("parallelism must be >= 1");
            }
            this.parallelism = parallelism;
            return this;
        }

        /** How long a group may stay idle before its bookkeeping is released. */
        public Builder idleTimeout(Duration idleTimeout) {
            Objects.requireNonNull(idleTimeout, "idleTimeout");
            if (idleTimeout.isNegative() || idleTimeout.isZero()) {
                throw new IllegalArgumentException("idleTimeout must be positive");
            }
            this.idleTimeout = idleTimeout;
            return this;
        }

        /** Callback invoked for every task failure. */
        public Builder failureHandler(TaskFailureHandler failureHandler) {
            this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
            return this;
        }

        public GroupedExecutor build() {
            return new GroupedExecutor(this);
        }
    }

    /** Per-group mutable state; all fields guarded by {@code synchronized (this)}. */
    private static final class GroupState {
        private final Deque<Runnable> queue = new ArrayDeque<>();
        private boolean drainerScheduled;
        private long lastActiveNanos = System.nanoTime();
    }
}
