package com.charlieworld.kafkatomysql.runner.mysql;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.Runner;
import com.charlieworld.kafkatomysql.dto.kafkadata.EventData;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 27.
 */
public class MySqlRunner extends Runner {

    private String tableName;
    private String database;
    private EventData eventData = null;
    private DbConnector mySqlConnector = null;

    public MySqlRunner(String tableName, String database, EventData eventData, DbConnector mySqlConnector) {
        this.tableName = tableName;
        this.database = database;
        this.eventData = eventData;
        this.mySqlConnector = mySqlConnector;
    }

    public String getInsertQuery() {
        return String.format(
                "insert into %s.%s values(%d, '%s', '%s', '%s');",
                this.database,
                this.tableName,
                this.eventData.getEventId(),
                this.eventData.getEventTimestamp(),
                this.eventData.getServiceCode(),
                this.eventData.getEventContext()
        );
    }

    public void putKafkaData(EventData eventData) {
        this.eventData = eventData;
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
