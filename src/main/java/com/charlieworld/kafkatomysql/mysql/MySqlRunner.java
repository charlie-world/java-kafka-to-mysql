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
public class MySqlRunner {

    private String tableName;
    private KafkaData kafkaData = null;
    private MySqlConnector mySqlConnector = null;

    public MySqlRunner(String tableName, KafkaData kafkaData, MySqlConnector mySqlConnector) {
        this.tableName = tableName;
        this.kafkaData = kafkaData;
        this.mySqlConnector = mySqlConnector;
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

    public int insertKafkaData(String sql) {
        return mySqlConnector.executeUpdate(sql);
    }

    public Runnable run() {
        return new Runnable() {
            public void run() {
                try {
                    insertKafkaData(getInsertQuery());
                } finally {
                    mySqlConnector.close();
                }
            }
        };
    }
}
