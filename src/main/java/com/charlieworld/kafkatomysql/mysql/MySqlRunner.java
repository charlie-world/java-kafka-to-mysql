package com.charlieworld.kafkatomysql.mysql;

import com.charlieworld.kafkatomysql.dto.KafkaData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
public class MySqlRunner {

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private String userName = null;
    private String passWord = null;
    private String host = null;
    private String tableName = null;
    private int port = 3306;

    private Connection connection = null;

    public MySqlRunner(String userName, String password, String host, int port, String tableName) {
        this.userName = userName;
        this.passWord = password;
        this.host = host;
        this.port = port;
        this.tableName = tableName;
    }

    public String getInsertQuery(KafkaData kafkaData) {
        String sql = "insert into" + this.tableName + String.format(
                "values(%s, %s, %s, %s);",
                Long.toString(kafkaData.getEventId()),
                kafkaData.getEventTimestamp(),
                kafkaData.getServiceCode(),
                kafkaData.getEventContext()
        );
        return sql;
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

    public int insertOp(KafkaData kafkaData) {
        int returnValue = -1;
        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(this.getDbUrl(), this.userName, this.passWord);
            Statement statement = connection.createStatement();
            returnValue = statement.executeUpdate(getInsertQuery(kafkaData));
            connection.close();
        } finally {
            System.out.println("db disconnnected...");
            return returnValue;
        }
    }
}
