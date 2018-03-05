package com.charlieworld.kafkatomysql.dto.runnerqueue;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.Runner;
import com.charlieworld.kafkatomysql.dto.KafkaData;
import com.charlieworld.kafkatomysql.dto.RunnerQueue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

public class MySqlRunnerQueue extends RunnerQueue {

    private final Queue<Runner> MY_SQL_RUNNER_QUEUE = new LinkedList<Runner>();
    private String tableName = null;
    private String database = null;
    private DbConnector mySqlConnector = null;

    public MySqlRunnerQueue(String tableName, String database, DbConnector mySqlConnector) {
        if (tableName == null || mySqlConnector == null) {
            throw new IllegalArgumentException("MySql runner queue, table name or MySql connector must not be null");
        } else {
            this.tableName = tableName;
            this.database = database;
            this.mySqlConnector = mySqlConnector;
        }
    }

    public boolean isEmpty() { return MY_SQL_RUNNER_QUEUE.isEmpty(); }

    public Runner dequeue() { return MY_SQL_RUNNER_QUEUE.remove(); }

    public void enqueue(Collection<KafkaData> collection) {
        for(KafkaData kafkaData: collection) {
            MY_SQL_RUNNER_QUEUE.add(kafkaData.mapToRunner(tableName, database, mySqlConnector));
        }
    }
}
