package com.charlieworld.kafkatomysql.dto;

import com.charlieworld.kafkatomysql.Runner;

import java.util.Collection;

public abstract class RunnerQueue {

    public abstract boolean isEmpty();

    public abstract Runner dequeue();

    public abstract void enqueue(Collection<KafkaData> collection);
}
