package com.charlieworld.kafkatomysql.runner.managequeue;

import com.charlieworld.kafkatomysql.Runner;
import com.charlieworld.kafkatomysql.dto.RunnerQueue;
import org.apache.kafka.common.errors.InterruptException;

import java.util.concurrent.ExecutorService;

public final class QueueManagingRunner extends Runner {

    private RunnerQueue mySqlRunnerQueue = null;
    private ExecutorService executorService = null;

    public QueueManagingRunner(RunnerQueue mySqlRunnerQueue, ExecutorService executorService) {
        if (mySqlRunnerQueue == null) {
            throw new IllegalArgumentException("Runner Queue must not be null value");
        } else if (executorService == null) {
            throw new IllegalArgumentException("ExecutorService must not be null value");
        } else {
            this.mySqlRunnerQueue = mySqlRunnerQueue;
            this.executorService = executorService;
        }
    }

    public void run() {
        while(!Thread.interrupted()) {
            try {
                if (!mySqlRunnerQueue.isEmpty()) {
                    runMySql(mySqlRunnerQueue.dequeue());
                }
            } catch (InterruptException ie) {
                ie.printStackTrace();
                this.close();
            }
        }
    }

    public void runMySql(Runner runner) {
        executorService.execute(runner);
    }
}
