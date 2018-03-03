package com.charlieworld.kafkatomysql.runner.managequeue;

import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;
import com.charlieworld.kafkatomysql.dto.runnerqueue.RunnerQueue;
import org.apache.kafka.common.errors.InterruptException;

import java.util.concurrent.ExecutorService;

public class ManageQueueRunner implements Runnable {

    private RunnerQueue runnerQueue = null;
    private ExecutorService executorService = null;

    public ManageQueueRunner(RunnerQueue runnerQueue, ExecutorService executorService) {
        if (runnerQueue == null) {
            throw new IllegalArgumentException("Runner Queue must not be null value");
        } else if (executorService == null) {
            throw new IllegalArgumentException("ExecutorService must not be null value");
        } else {
            this.runnerQueue = runnerQueue;
            this.executorService = executorService;
        }
    }

    public void run() {
        while(!Thread.interrupted()) {
            try {
                if (!runnerQueue.isEmpty()) {
                    runMySql(runnerQueue.dequeue());
                }
            } catch (InterruptException ie) {
                ie.printStackTrace();
            }
        }
    }

    public void runMySql(MySqlRunner mySqlRunner) {
        executorService.execute(mySqlRunner);
    }
}
