package com.example.gsb.executor;

/** Callback invoked for every task that fails. Must not throw. */
@FunctionalInterface
public interface FailureHandler<K> {
    void onFailure(TaskFailure<K> failure);
}
