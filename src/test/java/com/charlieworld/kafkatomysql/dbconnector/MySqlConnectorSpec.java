package com.charlieworld.kafkatomysql.dbconnector;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.dbconnector.MySqlConnector;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Writer Charlie Lee
 * Created at 2018. 3. 2.
 */
@RunWith(JUnit4.class)
public class MySqlConnectorSpec extends TestCase {

    String userName;
    String passWord;
    String host;
    int port;

    @Before
    public void before() {
        userName = "root";
        passWord = "password";
        host = "localhost";
        port = 3306;
    }

    @Test
    public void MySqlConnectorTest() {
        DbConnector mySqlConnector = new MySqlConnector(userName, passWord, host, port);

        String newHost = "NEW_HOST";
        int newPort = 3307;
        assertEquals(host, mySqlConnector.getHost());
        assertEquals(port, mySqlConnector.getPort());

        mySqlConnector.setHost(newHost);
        mySqlConnector.setPort(newPort);

        assertEquals(newHost, mySqlConnector.getHost());
        assertEquals(newPort, mySqlConnector.getPort());
    }

    @Test
    public void MySqlConnectorHandleErrorTest() {
        IllegalArgumentException illegalArgumentException = new IllegalArgumentException("user name, password, host must not be null value");
        try {
            new MySqlConnector(null, passWord, host, port);
        } catch (IllegalArgumentException ie) {
            assertEquals(illegalArgumentException.getMessage(), ie.getMessage());
        }
    }
}
