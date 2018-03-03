package com.charlieworld.kafkatomysql.runner.intervaltime;

import com.charlieworld.kafkatomysql.dto.kafkadata.KafkaData;
import com.charlieworld.kafkatomysql.runner.kafka.KafkaConsumeRunner;
import com.charlieworld.kafkatomysql.dto.runnerqueue.RunnerQueue;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;

public class IntervalTimeRunner implements Runnable {

    private long interval = 10;
    private RunnerQueue runnerQueue = null;
    private Lock mutex = null;
    private HashMap<String, KafkaData> hashMap = null;
    private KafkaConsumeRunner kafkaConsumeRunner = null;

    public IntervalTimeRunner(long interval,
                              RunnerQueue runnerQueue,
                              Lock mutex,
                              KafkaConsumeRunner kafkaConsumeRunner) {
        if (runnerQueue == null || mutex == null || kafkaConsumeRunner == null) {
            throw new IllegalArgumentException("Runner Queue, Mutex or KafkaConsumeRunner must not be null value");
        } else {
            this.interval = interval;
            this.runnerQueue = runnerQueue;
            this.mutex = mutex;
            this.hashMap = kafkaConsumeRunner.getHashMap();
            this.kafkaConsumeRunner = kafkaConsumeRunner;
        }
    }

    public HashMap<String, KafkaData> getHashMap() {
        return this.hashMap;
    }

    public Collection<KafkaData> resetHashMap(HashMap<String, KafkaData> newHashMap) {

        /**
         * @param newHashMap: to init hashMap
         * @return old hashMap values Collections
         * */

        HashMap<String, KafkaData> copyHashMap = this.hashMap;
        mutex.lock();
        this.hashMap = newHashMap;
        mutex.unlock();
        kafkaConsumeRunner.setHashMap(this.hashMap);
        return copyHashMap.values();
    }

    public void run() {
        while(!Thread.interrupted()) {
            try {
                Thread.sleep(interval * 60 * 1000);
                Collection<KafkaData> collection = resetHashMap(new HashMap<String, KafkaData>());
                runnerQueue.enqueue(collection);
            } catch (InterruptedException ie) {
                System.exit(1);
            }
        }
    }
}
