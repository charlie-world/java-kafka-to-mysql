package com.charlieworld.kafkatomysql.dto;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 28.
 */
public class KafkaDataBuilder {

    private long eventId = -1;
    private String eventTimestamp = null;
    private String serviceCode = null;
    private String eventContext = null;

    public KafkaDataBuilder() {}

    public KafkaDataBuilder eventId(long eventId) {
        this.eventId = eventId;
        return this;
    }

    public KafkaDataBuilder eventTimestamp(String eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
        return this;
    }

    public KafkaDataBuilder serviceCode(String serviceCode) {
        this.serviceCode = serviceCode;
        return this;
    }

    public KafkaDataBuilder eventContext(String eventContext) {
        this.eventContext = eventContext;
        return this;
    }

    public KafkaData build() {
        return new KafkaData(this.eventId, this.eventTimestamp, this.serviceCode, this.eventContext);
    }
}
