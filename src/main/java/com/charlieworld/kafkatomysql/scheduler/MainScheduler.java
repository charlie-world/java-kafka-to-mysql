package com.charlieworld.kafkatomysql.scheduler;

import com.charlieworld.kafkatomysql.dto.kafkadata.KafkaData;
import com.charlieworld.kafkatomysql.dto.runnerqueue.RunnerQueue;
import com.charlieworld.kafkatomysql.runner.intervaltime.IntervalTimeRunner;
import com.charlieworld.kafkatomysql.runner.intervaltime.IntervalTimeRunnerBuilder;
import com.charlieworld.kafkatomysql.runner.kafka.KafkaConsumeRunner;
import com.charlieworld.kafkatomysql.runner.kafka.KafkaConsumeRunnerBuilder;
import com.charlieworld.kafkatomysql.runner.managequeue.ManageQueueRunner;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlConnector;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MainScheduler implements Scheduler {

    private long interval = 10;
    private String tableName = null;
    private List<String> topics = null;
    private String bootstrapServers = null;
    private String groupId = null;

    private MySqlConnector mySqlConnector = null;

    private final Lock mutex = new ReentrantLock(true);
    private HashMap<String, KafkaData> hashMap = null;
    private RunnerQueue runnerQueue = null;
    private static final int numOfThreadPool = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService executorService = Executors.newFixedThreadPool(numOfThreadPool);

    public MainScheduler(Properties props) {
        this.mySqlConnector = getMySqlConnector(props);
        try {
            this.topics = Arrays.asList(props.getProperty("topics").trim().split(","));
            this.bootstrapServers = props.getProperty("bootstrap_servers");
            this.groupId = props.getProperty("group_id");
            this.tableName = props.getProperty("table_name");
            this.interval = Long.getLong(props.getProperty("interval_time"));
            this.runnerQueue = new RunnerQueue(
                    new LinkedList<MySqlRunner>(),
                    tableName,
                    mySqlConnector
            );
        } catch (NullPointerException ne) {
            ne.getCause();
        }
    }

    private MySqlConnector getMySqlConnector(Properties props) {
        String username = null;
        String password = null;
        String host = null;
        int port = 3306;
        MySqlConnector mysqlConnector = null;
        try {
            username = props.getProperty("username");
            password = props.getProperty("password");
            host = props.getProperty("host");
            port = Integer.getInteger(props.getProperty("port"));
            mysqlConnector = new MySqlConnector(username, password, host, port);
        } catch (IllegalArgumentException ie) {
            ie.printStackTrace();
        }
        return mysqlConnector;
    }

    public MainScheduler runMain() {

        KafkaConsumeRunner kafkaConsumeRunner = new KafkaConsumeRunnerBuilder()
                .topics(topics)
                .bootstrapServers(bootstrapServers)
                .groupId(groupId)
                .mutex(mutex)
                .build();

        hashMap = kafkaConsumeRunner.getHashMap();

        IntervalTimeRunner intervalTimeRunner = new IntervalTimeRunnerBuilder()
                .interval(interval)
                .runnerQueue(runnerQueue)
                .kafKaConsumeRunner(kafkaConsumeRunner)
                .mutex(mutex)
                .hashMap(hashMap)
                .build();

        ManageQueueRunner manageQueueRunner = new ManageQueueRunner(runnerQueue, executorService);

        executorService.execute(kafkaConsumeRunner);
        executorService.execute(intervalTimeRunner);
        executorService.execute(manageQueueRunner);

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
