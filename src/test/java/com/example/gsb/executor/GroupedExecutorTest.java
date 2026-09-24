package com.example.gsb.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class GroupedExecutorTest {

    private static final Duration IDLE_TIMEOUT = Duration.ofMillis(150);

    private GroupedExecutor<String> newExecutor(int maxConcurrency,
                                                ConcurrentLinkedQueue<TaskFailure<String>> failures) {
        return new GroupedExecutor<>(maxConcurrency, IDLE_TIMEOUT, failures::add);
    }

    @Test
    void sameGroupExecutesSequentiallyInSubmissionOrder() throws Exception {
        ConcurrentLinkedQueue<TaskFailure<String>> failures = new ConcurrentLinkedQueue<>();
        try (GroupedExecutor<String> executor = newExecutor(4, failures)) {
            List<String> events = new CopyOnWriteArrayList<>();
            CompletableFuture<?>[] futures = new CompletableFuture[10];
            for (int i = 0; i < 10; i++) {
                int index = i;
                futures[i] = executor.submit("order", () -> {
                    events.add("start-" + index);
                    Thread.sleep(20);
                    events.add("end-" + index);
                });
            }
            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);

            List<String> expected = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 10; i++) {
                expected.add("start-" + i);
                expected.add("end-" + i);
            }
            assertThat(events).containsExactlyElementsOf(expected);
        }
        assertThat(failures).isEmpty();
    }

    @Test
    void differentGroupsRunInParallel() throws Exception {
        ConcurrentLinkedQueue<TaskFailure<String>> failures = new ConcurrentLinkedQueue<>();
        try (GroupedExecutor<String> executor = newExecutor(4, failures)) {
            CountDownLatch bothStarted = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);

            CompletableFuture<Void> a = executor.submit("A", () -> {
                bothStarted.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("group B never started: no cross-group parallelism");
                }
            });
            CompletableFuture<Void> b = executor.submit("B", () -> {
                bothStarted.countDown();
                release.countDown();
            });

            assertThat(bothStarted.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture.allOf(a, b).get(10, TimeUnit.SECONDS);
        }
        assertThat(failures).isEmpty();
    }

    @Test
    void globalConcurrencyLimitIsRespected() throws Exception {
        ConcurrentLinkedQueue<TaskFailure<String>> failures = new ConcurrentLinkedQueue<>();
        int maxConcurrency = 2;
        try (GroupedExecutor<String> executor = newExecutor(maxConcurrency, failures)) {
            AtomicInteger inFlight = new AtomicInteger();
            AtomicInteger maxObserved = new AtomicInteger();
            CompletableFuture<?>[] futures = new CompletableFuture[20];
            for (int i = 0; i < 20; i++) {
                futures[i] = executor.submit("group-" + i, () -> {
                    int current = inFlight.incrementAndGet();
                    maxObserved.accumulateAndGet(current, Math::max);
                    Thread.sleep(50);
                    inFlight.decrementAndGet();
                });
            }
            CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);

            assertThat(maxObserved.get()).isEqualTo(maxConcurrency);
            assertThat(executor.completedTaskCount()).isEqualTo(20);
        }
        assertThat(failures).isEmpty();
    }

    @Test
    void idleGroupIsReclaimedAndRecreatedOnNewSubmission() throws Exception {
        ConcurrentLinkedQueue<TaskFailure<String>> failures = new ConcurrentLinkedQueue<>();
        try (GroupedExecutor<String> executor = newExecutor(2, failures)) {
            executor.submit("ephemeral", () -> { }).get(10, TimeUnit.SECONDS);
            assertThat(executor.activeGroupCount()).isEqualTo(1);

            // Wait longer than the idle timeout: the group must be reclaimed.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (executor.activeGroupCount() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(executor.activeGroupCount()).isZero();

            // A new task for the same key must recreate the group and run.
            AtomicInteger ran = new AtomicInteger();
            executor.submit("ephemeral", ran::incrementAndGet).get(10, TimeUnit.SECONDS);
            assertThat(ran.get()).isEqualTo(1);
            assertThat(executor.activeGroupCount()).isEqualTo(1);
        }
        assertThat(failures).isEmpty();
    }

    @Test
    void failingTaskDoesNotAffectSiblingsOrOtherGroups() throws Exception {
        ConcurrentLinkedQueue<TaskFailure<String>> failures = new ConcurrentLinkedQueue<>();
        try (GroupedExecutor<String> executor = newExecutor(2, failures)) {
            AtomicInteger afterFailure = new AtomicInteger();
            AtomicInteger otherGroup = new AtomicInteger();

            CompletableFuture<Void> boom = executor.submit("A", () -> {
                throw new IllegalStateException("boom");
            });
            CompletableFuture<Void> aNext = executor.submit("A", afterFailure::incrementAndGet);
            CompletableFuture<Void> b = executor.submit("B", otherGroup::incrementAndGet);

            assertThatThrownBy(() -> boom.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            aNext.get(10, TimeUnit.SECONDS);
            b.get(10, TimeUnit.SECONDS);

            assertThat(afterFailure.get()).isEqualTo(1);
            assertThat(otherGroup.get()).isEqualTo(1);
            assertThat(executor.failedTaskCount()).isEqualTo(1);
            assertThat(executor.completedTaskCount()).isEqualTo(2);

            assertThat(failures).hasSize(1);
            TaskFailure<String> failure = failures.peek();
            assertThat(failure.key()).isEqualTo("A");
            assertThat(failure.error()).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void gracefulShutdownDrainsQueueAndRejectsNewTasks() throws Exception {
        ConcurrentLinkedQueue<TaskFailure<String>> failures = new ConcurrentLinkedQueue<>();
        GroupedExecutor<String> executor = newExecutor(2, failures);
        AtomicInteger executed = new AtomicInteger();
        CountDownLatch firstRunning = new CountDownLatch(1);

        executor.submit("A", () -> {
            firstRunning.countDown();
            Thread.sleep(100);
            executed.incrementAndGet();
        });
        for (int i = 0; i < 5; i++) {
            executor.submit("A", executed::incrementAndGet);
        }
        executor.submit("B", executed::incrementAndGet);
        assertThat(firstRunning.await(5, TimeUnit.SECONDS)).isTrue();

        executor.shutdown();
        assertThatThrownBy(() -> executor.submit("A", () -> { }))
                .isInstanceOf(RejectedExecutionException.class);

        boolean terminated = executor.awaitTermination(Duration.ofSeconds(10));
        assertThat(terminated).isTrue();
        assertThat(executed.get()).isEqualTo(7);
        assertThat(failures).isEmpty();
    }

    @Test
    void awaitTerminationTimesOutWhenTasksOutlastTimeout() throws Exception {
        ConcurrentLinkedQueue<TaskFailure<String>> failures = new ConcurrentLinkedQueue<>();
        GroupedExecutor<String> executor = newExecutor(1, failures);
        CountDownLatch blocker = new CountDownLatch(1);
        executor.submit("slow", () -> {
            if (!blocker.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test latch not released");
            }
        });
        executor.shutdown();
        assertThat(executor.awaitTermination(Duration.ofMillis(200))).isFalse();
        blocker.countDown();
        assertThat(executor.awaitTermination(Duration.ofSeconds(10))).isTrue();
    }
}
