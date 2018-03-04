package com.charlieworld.kafkatomysql.dto.kafkadata;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.dto.KafkaData;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;

import java.util.NoSuchElementException;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 28.
 */
public final class EventData extends KafkaData {

    private long eventId;
    private String eventTimestamp;
    private String serviceCode;
    private String eventContext;

    public EventData(long eventId, String eventTimestamp, String serviceCode, String eventContext) {
        if (eventTimestamp != null) {
            this.eventId = eventId;
            this.eventTimestamp = eventTimestamp;
            this.serviceCode = serviceCode;
            this.eventContext = eventContext;
        } else {
            throw new IllegalArgumentException("event timestamp required");
        }
    }

    public long getEventId() {
        return this.eventId;
    }

    public String getEventTimestamp() {
        if (this.eventTimestamp == null) {
            throw new NoSuchElementException("event timestamp is not exist");
        } else {
            return this.eventTimestamp;
        }
    }

    public String getServiceCode() {
        return this.serviceCode;
    }

    public String getEventContext() {
        return this.eventContext;
    }

    public MySqlRunner mapToRunner(String tableName, String database, DbConnector mySqlConnector) {
        return new MySqlRunner(tableName, database,this, mySqlConnector);
    }
}
