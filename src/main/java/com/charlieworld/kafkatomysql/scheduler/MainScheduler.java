package com.charlieworld.kafkatomysql.scheduler;

import com.charlieworld.kafkatomysql.dto.runnerqueue.RunnerQueue;
import com.charlieworld.kafkatomysql.runner.intervaltime.IntervalTimeRunner;
import com.charlieworld.kafkatomysql.runner.intervaltime.IntervalTimeRunnerBuilder;
import com.charlieworld.kafkatomysql.runner.kafka.KafkaConsumeRunner;
import com.charlieworld.kafkatomysql.runner.kafka.KafkaConsumeRunnerBuilder;
import com.charlieworld.kafkatomysql.runner.managequeue.ManageQueueRunner;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlConnector;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MainScheduler implements Scheduler {

    private long interval = 10;
    private String tableName = null;
    private String databse = null;

    private MySqlConnector mySqlConnector = null;
    private KafkaConsumeRunner kafkaConsumeRunner = null;
    private IntervalTimeRunner intervalTimeRunner = null;
    private RunnerQueue runnerQueue = null;

    private final Lock mutex = new ReentrantLock(true);
    private static final int numOfThreadPool = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService executorService = Executors.newFixedThreadPool(numOfThreadPool);

    public MainScheduler() {
        try {
            InputStream input = ClassLoader.class.getResourceAsStream("/config.properties");
            Properties props = new Properties();
            props.load(input);
            this.mySqlConnector = initMySqlConnector(props);
            this.kafkaConsumeRunner = initKafkaConsumeRunner(props);
            this.tableName = props.getProperty("table_name");
            this.databse = props.getProperty("database");
            this.runnerQueue = new RunnerQueue(
                    tableName,
                    databse,
                    mySqlConnector
            );
            this.intervalTimeRunner = initIntervalTimeRunner(props);
            input.close();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    private IntervalTimeRunner initIntervalTimeRunner(Properties props) {
        long interval = Long.valueOf(props.getProperty("interval_time"));

        return new IntervalTimeRunnerBuilder()
                .interval(interval)
                .runnerQueue(runnerQueue)
                .kafKaConsumeRunner(kafkaConsumeRunner)
                .mutex(mutex)
                .build();
    }

    private KafkaConsumeRunner initKafkaConsumeRunner(Properties props) {
        List<String> topics = Arrays.asList(props.getProperty("topics").trim().split(","));
        String bootstrapServers = props.getProperty("bootstrap_servers");
        String groupId = props.getProperty("group_id");

        return new KafkaConsumeRunnerBuilder()
                .topics(topics)
                .bootstrapServers(bootstrapServers)
                .groupId(groupId)
                .mutex(mutex)
                .build();
    }

    private MySqlConnector initMySqlConnector(Properties props) {
        String username = props.getProperty("username");
        String password = props.getProperty("password");
        String host = props.getProperty("host");
        int port = Integer.valueOf(props.getProperty("port"));
        return new MySqlConnector(username, password, host, port);
    }

    public MainScheduler runMain() {
        executorService.execute(kafkaConsumeRunner);
        executorService.execute(intervalTimeRunner);
        executorService.execute(new ManageQueueRunner(runnerQueue, executorService));
        System.out.println("Start Main Scheduler...");
        return this;
    }

    public void shutdown() {
        executorService.shutdown();
    }

    public void await(long minutes) {
        try {
            executorService.awaitTermination(minutes, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            executorService.shutdown();
        }
    }

    public void await() {
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            executorService.shutdown();
        }
    }
}
