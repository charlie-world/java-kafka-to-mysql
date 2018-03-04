package com.charlieworld.kafkatomysql.dto.kafkadata;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 28.
 */
public class EventDataBuilder {

    private long eventId = -1;
    private String eventTimestamp = null;
    private String serviceCode = null;
    private String eventContext = null;

    public EventDataBuilder() {}

    public EventDataBuilder eventId(long eventId) {
        this.eventId = eventId;
        return this;
    }

    public EventDataBuilder eventTimestamp(String eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
        return this;
    }

    public EventDataBuilder serviceCode(String serviceCode) {
        this.serviceCode = serviceCode;
        return this;
    }

    public EventDataBuilder eventContext(String eventContext) {
        this.eventContext = eventContext;
        return this;
    }

    public EventData build() {
        return new EventData(this.eventId, this.eventTimestamp, this.serviceCode, this.eventContext);
    }
}
