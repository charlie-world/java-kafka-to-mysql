package com.charlieworld.kafkatomysql.runner;

import com.charlieworld.kafkatomysql.consumer.kafkaconsumer.KafkaConsumeRunner;
import com.charlieworld.kafkatomysql.dto.KafkaData;
import com.charlieworld.kafkatomysql.dto.kafkadata.EventData;
import junit.framework.TestCase;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writer Charlie Lee
 * Created at 2018. 3. 1.
 */
@RunWith(JUnit4.class)
public class KafkaConsumeRunnerSpec extends TestCase {

    List<String> topics = null;
    String bootstrapServers = null;
    String groupId = null;
    KafkaConsumeRunner kafkaConsumeRunner = null;
    EventData eventData = null;

    long eventId = 1;
    String timestamp = "2018-01-01";
    String eventContext = "EVENT_CONTEXT";
    String serviceCode = "SERVICE_CODE";
    HashMap<String, KafkaData> hashMap = new HashMap<String, KafkaData>();
    Lock mutex = new ReentrantLock(true);

    @Before
    public void before() {
        topics = Arrays.asList("test-topic");
        bootstrapServers = "localhost:9035";
        groupId = "test-group-id";
        kafkaConsumeRunner = new KafkaConsumeRunner(topics, bootstrapServers, groupId, mutex);
        eventData = new EventData(eventId, timestamp, serviceCode, eventContext);
    }

    @Test
    public void KafkaSubscriberInitTest() {
        Properties expectedProps = new Properties();
        expectedProps.put("bootstrap.servers", bootstrapServers);
        expectedProps.put("group.id", groupId);
        expectedProps.put("key.deserializer", StringDeserializer.class.getName());
        expectedProps.put("value.deserializer", StringDeserializer.class.getName());

        assertEquals(kafkaConsumeRunner.getTopics(), topics);
        assertEquals(kafkaConsumeRunner.getBootstrapServers(), bootstrapServers);
        assertEquals(kafkaConsumeRunner.getGroupId(), groupId);
        assertEquals(kafkaConsumeRunner.getProps(), expectedProps);
    }

    @Test
    public void KafkaSubscriberJsonParseTest() {
        String fullBody = "{event_id: 1, event_timestamp: '2018-01-01', service_code: 'SERVICE_CODE', event_context: 'EVENT_CONTEXT'}";
        String bodyWithNull = "{event_id: 1, event_timestamp: '2018-01-01', service_code: 'SERVICE_CODE'}";

        EventData expectedFull = kafkaConsumeRunner.parseKafkaBody(fullBody);
        EventData expectedWithNull = kafkaConsumeRunner.parseKafkaBody(bodyWithNull);

        assertEquals(eventData.getEventId(), expectedFull.getEventId());
        assertEquals(eventData.getEventTimestamp(), expectedFull.getEventTimestamp());
        assertEquals(eventData.getServiceCode(), expectedFull.getServiceCode());
        assertEquals(eventData.getEventContext(), expectedFull.getEventContext());

        assertEquals(eventData.getEventId(), expectedWithNull.getEventId());
        assertEquals(eventData.getEventTimestamp(), expectedWithNull.getEventTimestamp());
        assertEquals(eventData.getServiceCode(), expectedWithNull.getServiceCode());
        assertEquals(null, expectedWithNull.getEventContext());
    }

    @Test
    public void KafkaSubscriberPutHashMapTest() {
        EventData otherEventData = new EventData(eventId, timestamp, null, null);
        kafkaConsumeRunner.putKafkaDataToHashMap(eventData);
        assertEquals(eventData, kafkaConsumeRunner.getHashMap().get(String.valueOf(eventData.getEventId())));

        // update based on event id
        kafkaConsumeRunner.putKafkaDataToHashMap(otherEventData);
        assertEquals(otherEventData, kafkaConsumeRunner.getHashMap().get(String.valueOf(eventData.getEventId())));
    }
}
