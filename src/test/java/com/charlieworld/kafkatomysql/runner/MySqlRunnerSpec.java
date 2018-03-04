package com.charlieworld.kafkatomysql.runner;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.dbconnector.MySqlConnector;
import com.charlieworld.kafkatomysql.dto.kafkadata.EventData;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Statement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
@RunWith(JUnit4.class)
public class MySqlRunnerSpec extends TestCase {

    String userName;
    String passWord;
    String host;
    int port;
    String tableName;
    String database;
    MySqlRunner mySqlRunner;
    DbConnector mySqlConnector;
    EventData eventData;

    long eventId = 1;
    String timestamp = "2018-01-01";
    String eventContext = "EVENT_CONTEXT";
    String serviceCode = "SERVICE_CODE";

    @Before
    public void before() {
        userName = "root";
        passWord = "password";
        host = "localhost";
        tableName = "TEST_TABLE";
        database = "TEST_DB";
        port = 3306;
        eventData = new EventData(eventId, timestamp, serviceCode, eventContext);
    }

    @Test
    public void mySqlRunnerPutKafkaDataTest() {
        mySqlConnector = new MySqlConnector(userName, passWord, host, port);
        mySqlRunner = new MySqlRunner(tableName, database, eventData, mySqlConnector);
        mySqlRunner.putKafkaData(eventData);
        String expectedSql = "insert into TEST_DB.TEST_TABLE values(1, '2018-01-01', 'SERVICE_CODE', 'EVENT_CONTEXT');";

        assertEquals(expectedSql, mySqlRunner.getInsertQuery());
    }

    @Test
    public void mySqlRunnerInsertTest() {
        mySqlConnector = mock(DbConnector.class);
        mySqlRunner = new MySqlRunner(tableName, database, eventData, mySqlConnector);
        Statement statement = mock(Statement.class);
        String sql = mySqlRunner.getInsertQuery();

        when(mySqlConnector.getStatement()).thenReturn(statement);
        when(mySqlConnector.executeUpdate(sql)).thenReturn(2);

        assertEquals(2, mySqlRunner.insertKafkaData(sql));
    }
}
