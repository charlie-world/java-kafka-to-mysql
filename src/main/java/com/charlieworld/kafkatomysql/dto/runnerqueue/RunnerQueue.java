package com.charlieworld.kafkatomysql.dto.runnerqueue;

import com.charlieworld.kafkatomysql.dto.kafkadata.KafkaData;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlConnector;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

public class RunnerQueue {

    private Queue<MySqlRunner> MY_SQL_RUNNER_QUEUE = new LinkedList<MySqlRunner>();
    private String tableName = null;
    private MySqlConnector mySqlConnector = null;

    public RunnerQueue(String tableName, MySqlConnector mySqlConnector) {
        if (tableName == null || mySqlConnector == null) {
            throw new IllegalArgumentException("MySql runner queue, table name or MySql connector must not be null");
        } else {
            this.tableName = tableName;
            this.mySqlConnector = mySqlConnector;
        }
    }

    public boolean isEmpty() { return MY_SQL_RUNNER_QUEUE.isEmpty(); }

    public MySqlRunner dequeue() { return MY_SQL_RUNNER_QUEUE.remove(); }

    public void enqueue(Collection<KafkaData> collectionOfKafkaData) {
        for(KafkaData kafkaData: collectionOfKafkaData) {
            MY_SQL_RUNNER_QUEUE.add(kafkaData.mapToMySqlRunner(tableName, mySqlConnector));
        }
    }
}
