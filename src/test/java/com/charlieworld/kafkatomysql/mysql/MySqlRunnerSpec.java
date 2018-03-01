package com.charlieworld.kafkatomysql.mysql;

import com.charlieworld.kafkatomysql.dto.KafkaData;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
@RunWith(JUnit4.class)
public class MySqlRunnerSpec extends TestCase {

    String userName = null;
    String passWord = null;
    String host = null;
    int port = 3306;
    String tableName = null;

    @Before
    public void before() {
        userName = "root";
        passWord = "password";
        host = "localhost";
        tableName = "TEST_TABLE";
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
        KafkaData mockedKafkaData = mock(KafkaData.class);

        mySqlRunner.setHost(host);
        mySqlRunner.setPort(port);

        when(mySqlRunner.insertOp(mockedKafkaData)).thenReturn(1);

        assertEquals(mySqlRunner.insertOp(mockedKafkaData), 1);
    }
}
