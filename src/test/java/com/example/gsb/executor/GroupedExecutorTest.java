package com.example.gsb.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GroupedExecutorTest {

    private GroupedExecutor executor;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (executor != null) {
            executor.close();
        }
    }

    private GroupedExecutor newExecutor(int parallelism) {
        executor = GroupedExecutor.builder()
                .parallelism(parallelism)
                .idleTimeout(Duration.ofMillis(150))
                .build();
        return executor;
    }

    @Test
    void sameGroupExecutesSequentiallyInSubmissionOrder() throws Exception {
        GroupedExecutor exec = newExecutor(4);
        int taskCount = 200;
        List<Integer> executed = new CopyOnWriteArrayList<>();
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(taskCount);

        for (int i = 0; i < taskCount; i++) {
            final int n = i;
            exec.submit("order-key", () -> {
                int now = concurrent.incrementAndGet();
                maxConcurrent.accumulateAndGet(now, Math::max);
                try {
                    executed.add(n);
                } finally {
                    concurrent.decrementAndGet();
                    done.countDown();
                }
            });
        }

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(executed).containsExactlyElementsOf(
                java.util.stream.IntStream.range(0, taskCount).boxed().toList());
        assertThat(maxConcurrent.get()).isEqualTo(1);
    }

    @Test
    void differentGroupsExecuteInParallel() throws Exception {
        GroupedExecutor exec = newExecutor(4);
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        // Two groups, each with a blocking first task. If they ran serially the
        // latch would never reach zero before the timeout.
        exec.submit("g1", blockingTask(bothStarted, release, done));
        exec.submit("g2", blockingTask(bothStarted, release, done));

        assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
        release.countDown();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }

    private static Runnable blockingTask(CountDownLatch started, CountDownLatch release, CountDownLatch done) {
        return () -> {
            started.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            done.countDown();
        };
    }

    @Test
    void globalParallelismIsRespected() throws Exception {
        int parallelism = 2;
        GroupedExecutor exec = newExecutor(parallelism);
        int groupCount = 6;
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(groupCount);

        for (int i = 0; i < groupCount; i++) {
            exec.submit("group-" + i, () -> {
                int now = running.incrementAndGet();
                maxRunning.accumulateAndGet(now, Math::max);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    running.decrementAndGet();
                    done.countDown();
                }
            });
        }

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(maxRunning.get()).isLessThanOrEqualTo(parallelism);
        // And the limit is actually used (work did overlap up to the cap).
        assertThat(maxRunning.get()).isEqualTo(parallelism);
    }

    @Test
    void idleGroupIsReapedAndRecreatedOnNewTask() throws Exception {
        GroupedExecutor exec = newExecutor(2);
        CountDownLatch first = new CountDownLatch(1);
        exec.submit("reap-me", first::countDown);
        assertThat(first.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(exec.groupCount()).isEqualTo(1);

        // Wait for the idle timeout (150ms) plus reaper slack to reclaim the group.
        awaitCondition(() -> exec.groupCount() == 0, Duration.ofSeconds(5));

        // A new task for the same key must recreate the group and still run.
        CountDownLatch second = new CountDownLatch(1);
        exec.submit("reap-me", second::countDown);
        assertThat(second.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(exec.groupCount()).isEqualTo(1);
    }

    @Test
    void taskFailureIsIsolatedRecordedAndReported() throws Exception {
        List<TaskFailure> callbackFailures = new CopyOnWriteArrayList<>();
        executor = GroupedExecutor.builder()
                .parallelism(2)
                .idleTimeout(Duration.ofMillis(150))
                .failureHandler((key, task, error) -> callbackFailures.add(
                        new TaskFailure(key, task, error, java.time.Instant.now())))
                .build();

        CountDownLatch sameGroupFollowUp = new CountDownLatch(1);
        CountDownLatch otherGroup = new CountDownLatch(1);
        RuntimeException boom = new RuntimeException("boom");

        exec_submitFailing(boom);
        executor.submit("bad-group", sameGroupFollowUp::countDown);
        executor.submit("good-group", otherGroup::countDown);

        assertThat(sameGroupFollowUp.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(otherGroup.await(5, TimeUnit.SECONDS)).isTrue();

        awaitCondition(() -> executor.failures().size() == 1, Duration.ofSeconds(5));
        assertThat(executor.failures()).hasSize(1);
        assertThat(executor.failures().get(0).groupKey()).isEqualTo("bad-group");
        assertThat(executor.failures().get(0).error()).isSameAs(boom);
        assertThat(callbackFailures).hasSize(1);
        assertThat(callbackFailures.get(0).error()).isSameAs(boom);
    }

    private void exec_submitFailing(RuntimeException boom) {
        executor.submit("bad-group", () -> {
            throw boom;
        });
    }

    @Test
    void gracefulShutdownDrainsQueueAndRejectsNewTasks() throws Exception {
        GroupedExecutor exec = newExecutor(2);
        int taskCount = 20;
        ConcurrentLinkedQueue<Integer> executed = new ConcurrentLinkedQueue<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        // Block the workers briefly so tasks pile up in group queues.
        exec.submit("blocker", () -> {
            firstStarted.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();

        for (int i = 0; i < taskCount; i++) {
            final int n = i;
            exec.submit("key-" + (n % 4), () -> executed.add(n));
        }

        exec.shutdown();
        assertThatThrownBy(() -> exec.submit("late", () -> { }))
                .isInstanceOf(RejectedExecutionException.class);

        release.countDown();
        assertThat(exec.awaitTermination(Duration.ofSeconds(10))).isTrue();
        assertThat(executed).hasSize(taskCount);
        assertThat(exec.pendingCount()).isZero();
    }

    @Test
    void awaitTerminationTimesOutWhenWorkDoesNotFinish() throws Exception {
        GroupedExecutor exec = newExecutor(1);
        CountDownLatch release = new CountDownLatch(1);
        exec.submit("slow", () -> {
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        // Give the worker a moment to pick up the task.
        Thread.sleep(100);
        exec.shutdown();
        assertThat(exec.awaitTermination(Duration.ofMillis(200))).isFalse();
        release.countDown();
        assertThat(exec.awaitTermination(Duration.ofSeconds(5))).isTrue();
    }

    private static void awaitCondition(Check check, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (check.passed()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    @FunctionalInterface
    private interface Check {
        boolean passed();
    }
}
