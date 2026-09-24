package com.example.gsb.executor;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes tasks grouped by a business key. Tasks of the same key run strictly
 * in submission order on a single worker at a time; tasks of different keys may
 * run in parallel, bounded by a global concurrency limit. Groups that stay idle
 * for a configurable period are reclaimed and transparently recreated on the
 * next submission.
 */
public final class GroupedExecutor<K> implements AutoCloseable {

    private final ThreadPoolExecutor workers;
    private final ScheduledExecutorService reaper;
    private final ConcurrentHashMap<K, Group> groups = new ConcurrentHashMap<>();
    private final FailureHandler<K> failureHandler;
    private final long idleTimeoutNanos;

    private final Object terminationLock = new Object();
    private boolean accepting = true; // guarded by terminationLock
    private long pending;             // submitted but not yet finished tasks, guarded by terminationLock

    private final AtomicLong completedCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();

    public GroupedExecutor(int maxConcurrency, Duration idleTimeout, FailureHandler<K> failureHandler) {
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1");
        }
        Objects.requireNonNull(idleTimeout, "idleTimeout");
        if (idleTimeout.isNegative()) {
            throw new IllegalArgumentException("idleTimeout must be >= 0");
        }
        this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
        this.idleTimeoutNanos = idleTimeout.toNanos();
        this.workers = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                maxConcurrency, daemonFactory("grouped-executor-worker"));
        this.reaper = Executors.newSingleThreadScheduledExecutor(daemonFactory("grouped-executor-reaper"));
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong seq = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + seq.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * Submits a task for the given group key. Tasks of the same key execute in
     * submission order. The returned future completes normally on success and
     * exceptionally if the task throws.
     *
     * @throws RejectedExecutionException if the executor is shutting down
     */
    public CompletableFuture<Void> submit(K key, GroupedTask task) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (terminationLock) {
            if (!accepting) {
                throw new RejectedExecutionException("executor is shutting down");
            }
            pending++;
        }
        // compute() holds the bin lock for key, so enqueue and idle-reclaim are
        // mutually exclusive for the same key.
        groups.compute(key, (k, group) -> {
            if (group == null) {
                group = new Group(k);
            }
            group.enqueue(k, new QueuedTask(task, future));
            return group;
        });
        return future;
    }

    /** Stops accepting new tasks. Already submitted tasks continue to run. */
    public void shutdown() {
        synchronized (terminationLock) {
            accepting = false;
        }
    }

    /**
     * Waits until all tasks submitted before {@link #shutdown()} have finished,
     * then shuts down the worker and reaper threads.
     *
     * @return true if everything terminated within the timeout
     */
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        synchronized (terminationLock) {
            long remaining = timeout.toNanos();
            while (pending > 0) {
                if (remaining <= 0) {
                    return false;
                }
                long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
                int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
                terminationLock.wait(millis, nanos);
                remaining = deadlineNanos - System.nanoTime();
            }
        }
        workers.shutdown();
        reaper.shutdown();
        long remaining = deadlineNanos - System.nanoTime();
        if (!workers.awaitTermination(Math.max(remaining, 1), TimeUnit.NANOSECONDS)) {
            return false;
        }
        remaining = deadlineNanos - System.nanoTime();
        return reaper.awaitTermination(Math.max(remaining, 1), TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() throws InterruptedException {
        shutdown();
        awaitTermination(Duration.ofDays(1));
    }

    /** Number of groups currently tracked (idle-reclaimed groups are excluded). */
    public int activeGroupCount() {
        return groups.size();
    }

    public long completedTaskCount() {
        return completedCount.get();
    }

    public long failedTaskCount() {
        return failedCount.get();
    }

    private void drain(Group group) {
        for (;;) {
            QueuedTask queued = group.poll();
            if (queued == null) {
                scheduleReap(group);
                return;
            }
            try {
                queued.task.run();
                queued.future.complete(null);
                completedCount.incrementAndGet();
            } catch (Throwable error) {
                queued.future.completeExceptionally(error);
                failedCount.incrementAndGet();
                notifyFailure(group.key, queued.task, error);
            } finally {
                synchronized (terminationLock) {
                    if (--pending == 0) {
                        terminationLock.notifyAll();
                    }
                }
            }
        }
    }

    private void notifyFailure(K key, GroupedTask task, Throwable error) {
        try {
            failureHandler.onFailure(new TaskFailure<>(key, task, error));
        } catch (Throwable handlerError) {
            handlerError.printStackTrace();
        }
    }

    private void scheduleReap(Group group) {
        try {
            reaper.schedule(() -> groups.computeIfPresent(group.key, (k, current) -> {
            // Holds the bin lock for key, so no concurrent enqueue for this key.
            synchronized (current) {
                if (current.running || !current.queue.isEmpty()) {
                    return current; // active again, keep it
                }
                long idleFor = System.nanoTime() - current.lastActiveNanos;
                if (idleFor >= idleTimeoutNanos) {
                    return null; // remove from map: reclaim the idle group
                }
            }
            scheduleReap(current); // not idle long enough yet, check again later
            return current;
            }), idleTimeoutNanos, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException ignored) {
            // Reaper already shut down during graceful termination.
        }
    }

    private final class Group {
        final K key;
        final ArrayDeque<QueuedTask> queue = new ArrayDeque<>();
        boolean running;
        long lastActiveNanos;

        Group(K key) {
            this.key = key;
        }

        synchronized void enqueue(K key, QueuedTask queued) {
            lastActiveNanos = System.nanoTime();
            queue.addLast(queued);
            if (!running) {
                running = true;
                workers.execute(() -> drain(this));
            }
        }

        synchronized QueuedTask poll() {
            QueuedTask queued = queue.pollFirst();
            if (queued == null) {
                running = false;
            }
            return queued;
        }
    }

    private static final class QueuedTask {
        final GroupedTask task;
        final CompletableFuture<Void> future;

        QueuedTask(GroupedTask task, CompletableFuture<Void> future) {
            this.task = task;
            this.future = future;
        }
    }
}
