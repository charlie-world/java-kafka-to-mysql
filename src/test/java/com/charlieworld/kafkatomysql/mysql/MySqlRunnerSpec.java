package com.charlieworld.kafkatomysql.mysql;

import com.charlieworld.kafkatomysql.dto.KafkaData;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
    MySqlRunner mySqlRunner;
    KafkaData kafkaData;

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
        port = 3306;
        mySqlRunner = new MySqlRunner(this.userName, this.passWord, this.host, this.port, this.tableName);
        kafkaData = new KafkaData(eventId, timestamp, serviceCode, eventContext);
    }

    @Test
    public void mySqlRunnerInitTest() {
        assertEquals(mySqlRunner.getHost(), host);
        assertEquals(mySqlRunner.getPort(), port);
    }

    @Test
    public void mySqlRunnerPutKafkaDataTest() {
        mySqlRunner.putKafkaData(kafkaData);
        String expectedSql = "insert into TEST_TABLE values(1, 2018-01-01, SERVICE_CODE, EVENT_CONTEXT);";

        assertEquals(expectedSql, mySqlRunner.getInsertQuery());
    }
}
