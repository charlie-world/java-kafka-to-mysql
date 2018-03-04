package com.charlieworld.kafkatomysql.dbconnector;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Writer Charlie Lee
 * Created at 2018. 3. 2.
 */
public final class MySqlConnector extends DbConnector {

    private final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private String userName;
    private String passWord;
    private String host;
    private int port;

    private Connection connection = null;
    private Statement statement = null;

    public MySqlConnector(String userName, String passWord, String host, int port) {
        if (userName == null || passWord == null || host == null) {
            throw new IllegalArgumentException("user name, password, host must not be null value");
        } else {
            this.userName = userName;
            this.passWord = passWord;
            this.host = host;
            this.port = port;
        }
    }

    private String getDbUrl() {
        return String.format("jdbc:mysql://%s:%d", host, port);
    }

    public String getHost() { return this.host; }

    public void setHost(String host) {
        if (host == null) {
            throw new IllegalArgumentException("host value must not be null value");
        } else {
            this.host = host;
        }
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) { this.port = port; }

    public Statement getStatement() {
        try {
            Class.forName(this.JDBC_DRIVER);
            connection = DriverManager.getConnection(this.getDbUrl(), userName, passWord);
            statement = connection.createStatement();
        } catch (ClassNotFoundException ce) {
            ce.printStackTrace();
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return statement;
    }

    public int executeUpdate(String sql) {
        int returnValue = -1;
        try {
            returnValue = getStatement().executeUpdate(sql);
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return returnValue;
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}
