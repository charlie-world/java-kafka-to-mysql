package com.charlieworld.kafkatomysql.scheduler;

public interface Scheduler {

    public Scheduler runMain();

    public void shutdown();

    public void await();

    public void await(long minutes);
}
