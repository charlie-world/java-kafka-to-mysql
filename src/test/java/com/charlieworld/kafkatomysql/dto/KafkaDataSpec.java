package com.charlieworld.kafkatomysql.dto;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import junit.framework.TestCase;
import org.junit.Test;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 28.
 */
@RunWith(JUnit4.class)
public class KafkaDataSpec extends TestCase {
    @Test
    public void kafkaDataTest() {
        long eventId = 1;
        String timestamp = "2018-02-28T00:00:00.000Z";
        String serviceCode = "SERVICE_CODE";
        String eventContext = "EVENT_CONTEXT";

        KafkaData kafkaData = new KafkaDataBuilder()
                                    .eventId(eventId)
                                    .eventTimestamp(timestamp)
                                    .serviceCode(serviceCode)
                                    .eventContext(eventContext)
                                    .build();

        assertEquals(kafkaData.getEventId(), eventId);
        assertEquals(kafkaData.getEventTimestamp(), timestamp);
        assertEquals(kafkaData.getServiceCode(), serviceCode);
        assertEquals(kafkaData.getEventContext(), eventContext);
    }
}
