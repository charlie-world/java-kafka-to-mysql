package com.charlieworld.kafkatomysql.scheduler;

import com.charlieworld.kafkatomysql.dto.KafkaData;
import com.charlieworld.kafkatomysql.kafka.KafkaSubscriber;
import com.charlieworld.kafkatomysql.mysql.MySqlConnector;
import com.charlieworld.kafkatomysql.mysql.MySqlRunner;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Scheduler {

    private long interval = 10;
    private String tableName = null;
    private List<String> topics = null;
    private String bootstrapServers = null;
    private String groupId = null;

    private MySqlConnector mySqlConnector = null;
    private KafkaSubscriber kafkaSubscriber = null;
    private final Lock mutex = new ReentrantLock(true);

    private HashMap<String, KafkaData> hashMap = new HashMap<String, KafkaData>();
    private static final Queue<MySqlRunner> mySqlRunnerQueue = new LinkedList<MySqlRunner>();
    private static final int numOfThreadPool = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService executorService = Executors.newFixedThreadPool(numOfThreadPool);

    public Scheduler(Properties props) {
        this.mySqlConnector = getMySqlConnector(props);
        try {
            this.topics = Arrays.asList(props.getProperty("topics").trim().split(","));
            this.bootstrapServers = props.getProperty("bootstrap_servers");
            this.groupId = props.getProperty("group_id");
            this.tableName = props.getProperty("table_name");
            this.interval = Long.getLong(props.getProperty("interval_time"));
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
        } finally {
            mysqlConnector = new MySqlConnector(username, password, host, port);
        }
        return mysqlConnector;
    }

    public MySqlRunner dequeue() {
        return mySqlRunnerQueue.remove();
    }

    public boolean enqueue(Collection<KafkaData> collectionOfKafkaData) {
        for(KafkaData kafkaData: collectionOfKafkaData) {
            mySqlRunnerQueue.add(kafkaData.mapToMySqlRunner(tableName, mySqlConnector));
        }
        return true;
    }

    public void runMySql(MySqlRunner mySqlRunner) {
        executorService.execute(mySqlRunner.run());
    }

    public Runnable manageQueue() {
        return new Runnable() {
            public void run() {
                while(!Thread.interrupted()) {
                    if (!mySqlRunnerQueue.isEmpty()) {
                        runMySql(dequeue());
                    }
                }
            }
        };
    }

    private Collection<KafkaData> resetHashMap() {
        HashMap<String, KafkaData> copyHashMap = this.hashMap;
        mutex.lock();
        this.hashMap = new HashMap<String, KafkaData>();
        mutex.unlock();
        kafkaSubscriber.setHashMap(mutex, this.hashMap);
        return copyHashMap.values();
    }

    public Runnable intervalTime() {
        return new Runnable() {
          public void run() {
              while(!Thread.interrupted()) {
                  try {
                      Thread.sleep(interval * 60 * 1000);
                      Collection<KafkaData> collection = resetHashMap();
                      enqueue(collection);
                  } catch (InterruptedException ie) {
                      ie.getCause();
                  }
              }
          }
        };
    }

    public Scheduler run() {
        executorService.execute(new KafkaSubscriber(topics, bootstrapServers, groupId, hashMap));
        executorService.execute(intervalTime());
        executorService.execute(manageQueue());

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
