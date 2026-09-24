package com.example.gsb.executor;

/**
 * Callback invoked when a task throws. Implementations must be thread-safe and
 * should not block for long, since they run on a worker thread.
 */
@FunctionalInterface
public interface TaskFailureHandler {

    /**
     * @param groupKey the group the failed task belongs to
     * @param task     the task that threw
     * @param error    the exception or error thrown by the task
     */
    void onFailure(Object groupKey, Runnable task, Throwable error);
}
