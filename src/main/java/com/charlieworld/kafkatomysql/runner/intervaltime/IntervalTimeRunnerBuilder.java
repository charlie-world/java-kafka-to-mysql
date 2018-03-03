package com.charlieworld.kafkatomysql.runner.intervaltime;

import com.charlieworld.kafkatomysql.dto.kafkadata.KafkaData;
import com.charlieworld.kafkatomysql.dto.runnerqueue.RunnerQueue;
import com.charlieworld.kafkatomysql.runner.kafka.KafkaConsumeRunner;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;

public class IntervalTimeRunnerBuilder {

    private long interval = 10;
    private RunnerQueue runnerQueue = null;
    private Lock mutex = null;
    private HashMap<String, KafkaData> hashMap = null;
    private KafkaConsumeRunner kafkaConsumeRunner = null;

    public IntervalTimeRunnerBuilder() {}

    public IntervalTimeRunnerBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    public IntervalTimeRunnerBuilder runnerQueue(RunnerQueue runnerQueue) {
        if (runnerQueue == null) {
            throw new IllegalArgumentException("Runner Queue must be existed");
        } else {
            this.runnerQueue = runnerQueue;
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

    public IntervalTimeRunnerBuilder kafKaConsumeRunner(KafkaConsumeRunner kafkaConsumeRunner) {
        if (kafkaConsumeRunner == null) {
            throw new IllegalArgumentException("kafkaConsumeRunner value must be existed");
        } else {
            this.kafkaConsumeRunner = kafkaConsumeRunner;
        }

        return this;
    }

    public IntervalTimeRunner build() {
        return new IntervalTimeRunner(this.interval, this.runnerQueue, this.mutex, this.hashMap, this.kafkaConsumeRunner);
    }
}
