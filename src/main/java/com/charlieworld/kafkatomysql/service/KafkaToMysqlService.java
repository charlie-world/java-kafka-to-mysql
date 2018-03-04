package com.charlieworld.kafkatomysql.service;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.Runner;
import com.charlieworld.kafkatomysql.Service;
import com.charlieworld.kafkatomysql.consumer.ConsumerRunner;
import com.charlieworld.kafkatomysql.consumer.kafkaconsumer.KafkaConsumeRunner;
import com.charlieworld.kafkatomysql.dbconnector.MySqlConnector;
import com.charlieworld.kafkatomysql.dto.RunnerQueue;
import com.charlieworld.kafkatomysql.dto.runnerqueue.MySqlRunnerQueue;
import com.charlieworld.kafkatomysql.runner.intervaltime.IntervalTimeRunner;
import com.charlieworld.kafkatomysql.runner.managequeue.QueueManagingRunner;

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
    private ExecutorService executorService = null;

    private final Lock mutex = new ReentrantLock(true);

    public KafkaToMysqlService(String PROPERTIES_PATH) {
        try {
            File file = new File(PROPERTIES_PATH);
            InputStream input = new FileInputStream(file);
            Properties props = new Properties();
            props.load(input);
            this.mySqlConnector = initMySqlConnector(props);
            this.kafkaConsumeRunner = initKafkaConsumeRunner(props);
            this.tableName = props.getProperty("db.table_name");
            this.databse = props.getProperty("db.database");
            int threadPoolSize = Integer.valueOf(props.getProperty("threadPoolSize"));
            this.executorService = Executors.newFixedThreadPool(threadPoolSize);
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

        return new IntervalTimeRunner(interval, mySqlRunnerQueue, mutex, kafkaConsumeRunner);
    }

    private ConsumerRunner initKafkaConsumeRunner(Properties props) {
        List<String> topics = Arrays.asList(props.getProperty("consumer.topics").trim().split(","));
        String bootstrapServers = props.getProperty("consumer.bootstrap_servers");
        String groupId = props.getProperty("consumer.group_id");

        return new KafkaConsumeRunner(topics, bootstrapServers, groupId, mutex);
    }

    private DbConnector initMySqlConnector(Properties props) {
        String username = props.getProperty("db.username");
        String password = props.getProperty("db.password");
        String host = props.getProperty("db.host");
        int port = Integer.valueOf(props.getProperty("db.port"));
        return new MySqlConnector(username, password, host, port);
    }

    public Service start() {
        executorService.execute(kafkaConsumeRunner);
        executorService.execute(intervalTimeRunner);
        executorService.execute(new QueueManagingRunner(mySqlRunnerQueue, executorService));
        logger.log(Level.INFO, "Start Service...");
        return this;
    }

    public void shutdown() {
        executorService.shutdown();
        logger.log(Level.INFO, "Shutdown Service...");
    }
}
