package com.charlieworld.kafkatomysql.mysql;

import com.charlieworld.kafkatomysql.dto.KafkaData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
public class MySqlRunner implements Runnable {

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private String userName;
    private String passWord;
    private String host;
    private String tableName;
    private int port;
    private KafkaData kafkaData = null;

    public MySqlRunner(String userName, String password, String host, int port, String tableName) {
        this.userName = userName;
        this.passWord = password;
        this.host = host;
        this.port = port;
        this.tableName = tableName;
    }

    public String getInsertQuery() {
        String sql = null;
        try {
            sql = "insert into " + this.tableName + " " + String.format(
                    "values(%s, %s, %s, %s);",
                    Long.toString(this.kafkaData.getEventId()),
                    this.kafkaData.getEventTimestamp(),
                    this.kafkaData.getServiceCode(),
                    this.kafkaData.getEventContext()
            );
        } catch (NullPointerException ne) {
            ne.getCause();
        }
        return sql;
    }

    public void putKafkaData(KafkaData kafkaData) {
        this.kafkaData = kafkaData;
    }

    public String getDbUrl() {
        return String.format("jdbc:mysql://%s:%d", host, port);
    }

    public String getHost() { return this.host; }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int insertKafkaData(Statement statement, String sql) {
        int returnValue = -1;
        try {
            returnValue = statement.executeUpdate(sql);
        } catch (SQLException se) {
            se.getCause();
        }
        return returnValue;
    }

    public void run() {
        try {
            Class.forName(JDBC_DRIVER);
            Connection connection = DriverManager.getConnection(getDbUrl(), userName, passWord);
            Statement statement = connection.createStatement();
            insertKafkaData(statement, getInsertQuery());
            connection.close();
        } catch (ClassNotFoundException ce) {
            ce.getCause();
        } catch (SQLException se) {
            se.getCause();
        }
    }
}
