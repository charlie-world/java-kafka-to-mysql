package com.charlieworld.kafkatomysql.service;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.Runner;
import com.charlieworld.kafkatomysql.Service;
import com.charlieworld.kafkatomysql.consumer.ConsumerRunner;
import com.charlieworld.kafkatomysql.consumer.kafkaconsumer.KafkaConsumeRunnerBuilder;
import com.charlieworld.kafkatomysql.dbconnector.MySqlConnector;
import com.charlieworld.kafkatomysql.dto.RunnerQueue;
import com.charlieworld.kafkatomysql.dto.runnerqueue.MySqlRunnerQueue;
import com.charlieworld.kafkatomysql.runner.intervaltime.IntervalTimeRunner;
import com.charlieworld.kafkatomysql.runner.intervaltime.IntervalTimeRunnerBuilder;
import com.charlieworld.kafkatomysql.runner.managequeue.ManageQueueRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaToMysqlService implements Service {

    private Logger logger = Logger.getLogger(getClass().getName());
    private String tableName = null;
    private String databse = null;

    private DbConnector mySqlConnector = null;
    private ConsumerRunner kafkaConsumeRunner = null;
    private Runner intervalTimeRunner = null;
    private RunnerQueue mySqlRunnerQueue = null;

    private final Lock mutex = new ReentrantLock(true);
    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    public KafkaToMysqlService(String PROPERTIES_PATH) {
        try {
            File file = new File(PROPERTIES_PATH);
            InputStream input = new FileInputStream(file);
            Properties props = new Properties();
            props.load(input);
            this.mySqlConnector = initMySqlConnector(props);
            this.kafkaConsumeRunner = initKafkaConsumeRunner(props);
            this.tableName = props.getProperty("table_name");
            this.databse = props.getProperty("database");
            this.mySqlRunnerQueue = new MySqlRunnerQueue(
                    tableName,
                    databse,
                    mySqlConnector
            );
            this.intervalTimeRunner = initIntervalTimeRunner(props);
            input.close();
        } catch (IOException ie) {
            ie.printStackTrace();
            System.exit(1);
        }
    }

    private Runner initIntervalTimeRunner(Properties props) {
        long interval = Long.valueOf(props.getProperty("interval_time"));

        return new IntervalTimeRunnerBuilder()
                .interval(interval)
                .runnerQueue(mySqlRunnerQueue)
                .kafKaConsumeRunner(kafkaConsumeRunner)
                .mutex(mutex)
                .build();
    }

    private ConsumerRunner initKafkaConsumeRunner(Properties props) {
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

    private DbConnector initMySqlConnector(Properties props) {
        String username = props.getProperty("username");
        String password = props.getProperty("password");
        String host = props.getProperty("host");
        int port = Integer.valueOf(props.getProperty("port"));
        return new MySqlConnector(username, password, host, port);
    }

    public Service start() {
        executorService.execute(kafkaConsumeRunner);
        executorService.execute(intervalTimeRunner);
        executorService.execute(new ManageQueueRunner(mySqlRunnerQueue, executorService));
        logger.log(Level.INFO, "Start Service...");
        return this;
    }

    public void shutdown() {
        executorService.shutdown();
        logger.log(Level.INFO, "Shutdown Service...");
    }
}
