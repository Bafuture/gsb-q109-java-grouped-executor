package com.example.gsb.executor;

/** Describes a task that terminated with an exception. */
public record TaskFailure<K>(K key, GroupedTask task, Throwable error) {
}
