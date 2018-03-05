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
        String prefix = String.format(
                "insert into %s.%s values(%d, '%s'",
                this.database,
                this.tableName,
                this.eventData.getEventId(),
                this.eventData.getEventTimestamp()
        );

        String suffix;

        if (this.eventData.getServiceCode() == null) {
            if (this.eventData.getEventContext() == null) {
                suffix = ", NULL, NULL);";
            } else {
                suffix = String.format(", NULL, '%s');", this.eventData.getEventContext());
            }
        } else {
            if (this.eventData.getEventContext() == null) {
                suffix = String.format(", '%s', NULL);", this.eventData.getServiceCode());
            } else {
                suffix = String.format(", '%s', '%s');", this.eventData.getServiceCode(), this.eventData.getEventContext());
            }
        }

        return prefix + suffix;
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
