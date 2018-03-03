package com.charlieworld.kafkatomysql.runner.mysql;

import com.charlieworld.kafkatomysql.dto.kafkadata.KafkaData;

import java.sql.SQLException;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
public class MySqlRunner implements Runnable {

    private String tableName;
    private String database;
    private KafkaData kafkaData = null;
    private MySqlConnector mySqlConnector = null;

    public MySqlRunner(String tableName, String database, KafkaData kafkaData, MySqlConnector mySqlConnector) {
        this.tableName = tableName;
        this.database = database;
        this.kafkaData = kafkaData;
        this.mySqlConnector = mySqlConnector;
    }

    public String getInsertQuery() {
        String sql = null;
        try {
            sql = String.format(
                    "insert into %s.%s values(%d, '%s', '%s', '%s');",
                    this.database,
                    this.tableName,
                    this.kafkaData.getEventId(),
                    this.kafkaData.getEventTimestamp(),
                    this.kafkaData.getServiceCode(),
                    this.kafkaData.getEventContext()
            );
        } catch (NullPointerException ne) {
            ne.printStackTrace();
        }
        return sql;
    }

    public void putKafkaData(KafkaData kafkaData) {
        this.kafkaData = kafkaData;
    }

    public int insertKafkaData(String sql) {
        return mySqlConnector.executeUpdate(sql);
    }

    public void run() {
        try {
            insertKafkaData(getInsertQuery());
        } finally {
            mySqlConnector.close();
        }
    }
}
