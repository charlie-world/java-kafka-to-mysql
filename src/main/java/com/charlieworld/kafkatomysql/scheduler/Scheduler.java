package com.charlieworld.kafkatomysql.scheduler;

public interface Scheduler {

    public abstract Scheduler runMain();

    public abstract void shutdown();

    public abstract void await();

    public abstract void await(long minutes);
}
