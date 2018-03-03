package com.charlieworld.kafkatomysql.dto;

import com.charlieworld.kafkatomysql.dto.runnerqueue.RunnerQueue;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlConnector;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedList;
import java.util.Queue;

@RunWith(JUnit4.class)
public class RunnerQueueSpec extends TestCase {

    Queue<MySqlRunner> MY_SQL_RUNNER_QUEUE = null;
    String tableName = null;
    MySqlConnector mySqlConnector = null;

    String username = "USER";
    String password = "PASSWORD";
    String host = "localhost";
    int port = 3306;

    @Before
    public void before() {
        this.MY_SQL_RUNNER_QUEUE = new LinkedList<MySqlRunner>();
        this.tableName = "TEST_TABLE";
        this.mySqlConnector = new MySqlConnector(username, password, host, port);
    }

    @Test
    public void RunnerQueueTest() {
        RunnerQueue runnerQueue = new RunnerQueue(MY_SQL_RUNNER_QUEUE, tableName, mySqlConnector);

        assertEquals(true, runnerQueue.isEmpty());
    }
}
