package com.example.gsb.executor;

/** A unit of work submitted to a {@link GroupedExecutor}. May throw. */
@FunctionalInterface
public interface GroupedTask {
    void run() throws Exception;
}
