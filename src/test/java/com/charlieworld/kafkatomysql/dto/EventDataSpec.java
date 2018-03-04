package com.charlieworld.kafkatomysql.dto;

import com.charlieworld.kafkatomysql.dto.kafkadata.EventData;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Writer Charlie Lee
 * Created at 2018. 2. 28.
 */
@RunWith(JUnit4.class)
public class EventDataSpec extends TestCase {
    @Test
    public void EventDataSpec() {
        long eventId = 1;
        String timestamp = "2018-02-28T00:00:00.000Z";
        String serviceCode = "SERVICE_CODE";
        String eventContext = "EVENT_CONTEXT";
        EventData eventData = new EventData(eventId, timestamp, serviceCode, eventContext);

        assertEquals(eventData.getEventId(), eventId);
        assertEquals(eventData.getEventTimestamp(), timestamp);
        assertEquals(eventData.getServiceCode(), serviceCode);
        assertEquals(eventData.getEventContext(), eventContext);
    }
}
