package com.charlieworld.kafkatomysql.dto;

import com.charlieworld.kafkatomysql.dto.kafkadata.KafkaData;
import com.charlieworld.kafkatomysql.dto.runnerqueue.RunnerQueue;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlConnector;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;
import java.util.LinkedList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class RunnerQueueSpec extends TestCase {

    String tableName = null;
    MySqlConnector mySqlConnector = null;

    String username = "USER";
    String password = "PASSWORD";
    String host = "localhost";
    int port = 3306;

    @Before
    public void before() {
        this.tableName = "TEST_TABLE";
        this.mySqlConnector = new MySqlConnector(username, password, host, port);
    }

    @Test
    public void RunnerQueueTest() {
        KafkaData kafkaData = mock(KafkaData.class);
        MySqlRunner mySqlRunner = mock(MySqlRunner.class);

        when(kafkaData.mapToMySqlRunner(tableName, mySqlConnector)).thenReturn(mySqlRunner);

        /*
        *  init RunnerQueue and check isEmpty method
        * */
        RunnerQueue runnerQueue = new RunnerQueue(tableName, mySqlConnector);
        assertEquals(true, runnerQueue.isEmpty());

        /*
         *  enqueue the KafkaData collection and check dequeue method
         * */
        Collection<KafkaData> collection = new LinkedList<KafkaData>();
        collection.add(kafkaData);
        runnerQueue.enqueue(collection);

        assertEquals(kafkaData.mapToMySqlRunner(tableName, mySqlConnector), runnerQueue.dequeue());
    }
}
