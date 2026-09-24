package com.example.gsb.executor;

import java.time.Instant;
import java.util.Objects;

/** An immutable record of a single task failure. */
public final class TaskFailure {

    private final Object groupKey;
    private final Runnable task;
    private final Throwable error;
    private final Instant timestamp;

    public TaskFailure(Object groupKey, Runnable task, Throwable error, Instant timestamp) {
        this.groupKey = groupKey;
        this.task = Objects.requireNonNull(task, "task");
        this.error = Objects.requireNonNull(error, "error");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
    }

    public Object groupKey() {
        return groupKey;
    }

    public Runnable task() {
        return task;
    }

    public Throwable error() {
        return error;
    }

    public Instant timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "TaskFailure{groupKey=" + groupKey + ", error=" + error + ", timestamp=" + timestamp + '}';
    }
}
