package com.charlieworld.kafkatomysql.kafka;

import com.charlieworld.kafkatomysql.dto.KafkaData;
import junit.framework.TestCase;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.*;

/**
 * Writer Charlie Lee
 * Created at 2018. 3. 1.
 */
@RunWith(JUnit4.class)
public class KafkaSubscriberSpec extends TestCase {

    List<String> topics = null;
    String bootstrapServers = null;
    String groupId = null;
    KafkaSubscriber kafkaSubscriber = null;
    KafkaData kafkaData = null;

    long eventId = 1;
    String timestamp = "2018-01-01";
    String eventContext = "EVENT_CONTEXT";
    String serviceCode = "SERVICE_CODE";

    @Before
    public void before() {
        topics = Arrays.asList("test-topic");
        bootstrapServers = "localhost:9035";
        groupId = "test-group-id";
        kafkaSubscriber = new KafkaSubscriber(topics, bootstrapServers, groupId);
        kafkaData = new KafkaData(eventId, timestamp, serviceCode, eventContext);
    }

    @Test
    public void KafkaSubscriberInitTest() {
        Properties expectedProps = new Properties();
        expectedProps.put("bootstrap.servers", bootstrapServers);
        expectedProps.put("group.id", groupId);
        expectedProps.put("key.deserializer", StringDeserializer.class.getName());
        expectedProps.put("value.deserializer", StringDeserializer.class.getName());

        assertEquals(kafkaSubscriber.getTopics(), topics);
        assertEquals(kafkaSubscriber.getBootstrapServers(), bootstrapServers);
        assertEquals(kafkaSubscriber.getGroupId(), groupId);
        assertEquals(kafkaSubscriber.getProps(), expectedProps);
    }

    @Test
    public void KafkaSubscriberJsonParseTest() {
        String fullBody = "{event_id: 1, event_timestamp: '2018-01-01', service_code: 'SERVICE_CODE', event_context: 'EVENT_CONTEXT'}";
        String bodyWithNull = "{event_id: 1, event_timestamp: '2018-01-01', service_code: 'SERVICE_CODE'}";

        KafkaData expectedFull = kafkaSubscriber.parseKafkaBody(fullBody);
        KafkaData expectedWithNull = kafkaSubscriber.parseKafkaBody(bodyWithNull);

        assertEquals(kafkaData.getEventId(), expectedFull.getEventId());
        assertEquals(kafkaData.getEventTimestamp(), expectedFull.getEventTimestamp());
        assertEquals(kafkaData.getServiceCode(), expectedFull.getServiceCode());
        assertEquals(kafkaData.getEventContext(), expectedFull.getEventContext());

        assertEquals(kafkaData.getEventId(), expectedWithNull.getEventId());
        assertEquals(kafkaData.getEventTimestamp(), expectedWithNull.getEventTimestamp());
        assertEquals(kafkaData.getServiceCode(), expectedWithNull.getServiceCode());
        assertEquals(null, expectedWithNull.getEventContext());
    }

    @Test
    public void KafkaSubscriberEnqueueTest() {
        Queue<KafkaData> queue = new LinkedList<KafkaData>();
        kafkaSubscriber.setQueue(queue);
        kafkaSubscriber.enqueueKafkaData(kafkaData);

        assertEquals(kafkaData, kafkaSubscriber.dequeueKafkaData());
    }
}
