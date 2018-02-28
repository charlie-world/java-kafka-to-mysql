package com.charlieworld.kafkatomysql;

import com.charlieworld.kafkatomysql.dto.KafkaData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
public class MySqlRunner {

    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    String userName = null;
    String passWord = null;
    String host = null;
    String tableName = null;
    int port = 3306;

    Connection connection = null;

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

    public void insertOp(KafkaData kafkaData) {
        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(this.getDbUrl(), this.userName, this.passWord);
            Statement statement = connection.createStatement();
            statement.executeUpdate(getInsertQuery(kafkaData));
            connection.close();
        } catch (SQLException se1) {
            se1.printStackTrace();
        } catch (ClassNotFoundException ce1) {
            ce1.printStackTrace();
        } finally {
            System.out.println("db disconnnected...");
        }
    }
}
