package com.charlieworld.kafkatomysql.runner.intervaltime;

import com.charlieworld.kafkatomysql.consumer.ConsumerRunner;
import com.charlieworld.kafkatomysql.dto.KafkaData;
import com.charlieworld.kafkatomysql.dto.RunnerQueue;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;

public class IntervalTimeRunnerBuilder {

    private long interval = 10;
    private RunnerQueue mySqlRunnerQueue = null;
    private Lock mutex = null;
    private HashMap<String, KafkaData> hashMap = null;
    private ConsumerRunner kafkaConsumeRunner = null;

    public IntervalTimeRunnerBuilder() {}

    public IntervalTimeRunnerBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    public IntervalTimeRunnerBuilder runnerQueue(RunnerQueue mySqlRunnerQueue) {
        if (mySqlRunnerQueue == null) {
            throw new IllegalArgumentException("Runner Queue must be existed");
        } else {
            this.mySqlRunnerQueue = mySqlRunnerQueue;
        }

        return this;
    }

    public IntervalTimeRunnerBuilder mutex(Lock mutex) {
        if (mutex == null) {
            throw new IllegalArgumentException("Mutex value must be existed");
        } else {
            this.mutex = mutex;
        }

        return this;
    }

    public IntervalTimeRunnerBuilder hashMap(HashMap<String, KafkaData> hashMap) {
        if (hashMap == null) {
            throw new IllegalArgumentException("HashMap value must be existed");
        } else {
            this.hashMap = hashMap;
        }

        return this;
    }

    public IntervalTimeRunnerBuilder kafKaConsumeRunner(ConsumerRunner kafkaConsumeRunner) {
        if (kafkaConsumeRunner == null) {
            throw new IllegalArgumentException("kafkaConsumeRunner value must be existed");
        } else {
            this.kafkaConsumeRunner = kafkaConsumeRunner;
        }

        return this;
    }

    public IntervalTimeRunner build() {
        return new IntervalTimeRunner(this.interval, this.mySqlRunnerQueue, this.mutex, this.kafkaConsumeRunner);
    }
}
