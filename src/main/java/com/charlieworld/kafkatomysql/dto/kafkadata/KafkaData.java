package com.charlieworld.kafkatomysql.dto.kafkadata;

import com.charlieworld.kafkatomysql.runner.mysql.MySqlConnector;
import com.charlieworld.kafkatomysql.runner.mysql.MySqlRunner;

import java.util.NoSuchElementException;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 28.
 */
public class KafkaData {

    private long eventId;
    private String eventTimestamp;
    private String serviceCode;
    private String eventContext;

    public KafkaData(long eventId, String eventTimestamp, String serviceCode, String eventContext) {
        if (eventId > 0 && eventTimestamp != null) {
            this.eventId = eventId;
            this.eventTimestamp = eventTimestamp;
            this.serviceCode = serviceCode;
            this.eventContext = eventContext;
        } else if (eventId <= 0) {
            throw new IllegalArgumentException(String.format("event id (%d) must be positive number", eventId));
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

    public MySqlRunner mapToMySqlRunner(String tableName, String database, MySqlConnector mySqlConnector) {
        return new MySqlRunner(tableName, database,this, mySqlConnector);
    }
}