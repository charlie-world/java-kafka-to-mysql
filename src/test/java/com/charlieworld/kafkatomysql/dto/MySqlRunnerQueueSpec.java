package com.charlieworld.kafkatomysql.dto;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.dbconnector.MySqlConnector;
import com.charlieworld.kafkatomysql.dto.kafkadata.EventData;
import com.charlieworld.kafkatomysql.dto.runnerqueue.MySqlRunnerQueue;
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
public class MySqlRunnerQueueSpec extends TestCase {

    String tableName = null;
    String database = null;
    DbConnector mySqlConnector = null;

    String username = "USER";
    String password = "PASSWORD";
    String host = "localhost";
    int port = 3306;

    @Before
    public void before() {
        this.tableName = "TEST_TABLE";
        this.database = "TEST_DB";
        this.mySqlConnector = new MySqlConnector(username, password, host, port);
    }

    @Test
    public void RunnerQueueTest() {
        EventData eventData = mock(EventData.class);
        MySqlRunner mySqlRunner = mock(MySqlRunner.class);

        when(eventData.mapToRunner(tableName, database, mySqlConnector)).thenReturn(mySqlRunner);

        /*
        *  init MySqlRunnerQueue and check isEmpty method
        * */
        RunnerQueue mySqlRunnerQueue = new MySqlRunnerQueue(tableName, database, mySqlConnector);
        assertEquals(true, mySqlRunnerQueue.isEmpty());

        /*
         *  enqueue the EventData collection and check dequeue method
         * */
        Collection<KafkaData> collection = new LinkedList<KafkaData>();
        collection.add(eventData);
        mySqlRunnerQueue.enqueue(collection);

        assertEquals(eventData.mapToRunner(tableName, database, mySqlConnector), mySqlRunnerQueue.dequeue());
    }
}
