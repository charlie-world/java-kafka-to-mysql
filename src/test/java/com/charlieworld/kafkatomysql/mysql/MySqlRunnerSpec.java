package com.charlieworld.kafkatomysql.mysql;

import com.charlieworld.kafkatomysql.dto.KafkaData;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.Statement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

    @Before
    public void before() {
        userName = "root";
        passWord = "password";
        host = "localhost";
        tableName = "TEST_TABLE";
        port = 3306;
    }

    @Test
    public void mySqlRunnerInitTest() {
        MySqlRunner mySqlRunner = new MySqlRunner(this.userName, this.passWord, this.host, this.port, this.tableName);
        assertEquals(mySqlRunner.getHost(), host);
        assertEquals(mySqlRunner.getPort(), port);
    }

    @Test
    public void mySqlRunnerTest() {
        MySqlRunner mySqlRunner = mock(MySqlRunner.class);
        KafkaData kafkaData = mock(KafkaData.class);

        when(mySqlRunner.insertOp(kafkaData)).thenReturn(1);

        assertEquals(1, mySqlRunner.insertOp(kafkaData));
    }
}