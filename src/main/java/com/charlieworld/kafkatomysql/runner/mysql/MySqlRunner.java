package com.charlieworld.kafkatomysql.runner.mysql;

import com.charlieworld.kafkatomysql.dto.kafkadata.KafkaData;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
public class MySqlRunner implements Runnable {

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

    public void run() {
        try {
            insertKafkaData(getInsertQuery());
        } finally {
            mySqlConnector.close();
        }
    }
}
