package com.charlieworld.kafkatomysql.runner.kafka;

import com.charlieworld.kafkatomysql.dto.kafkadata.KafkaData;
import junit.framework.TestCase;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.*;
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
    KafkaData kafkaData = null;

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
        kafkaConsumeRunner = new KafkaConsumeRunnerBuilder()
                                .topics(topics)
                                .bootstrapServers(bootstrapServers)
                                .groupId(groupId)
                                .mutex(mutex)
                                .build();
        kafkaData = new KafkaData(eventId, timestamp, serviceCode, eventContext);
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

        KafkaData expectedFull = kafkaConsumeRunner.parseKafkaBody(fullBody);
        KafkaData expectedWithNull = kafkaConsumeRunner.parseKafkaBody(bodyWithNull);

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
    public void KafkaSubscriberPutHashMapTest() {
        KafkaData otherKafkaData = new KafkaData(eventId, timestamp, null, null);
        kafkaConsumeRunner.putKafkaDataToHashMap(kafkaData);
        assertEquals(kafkaData, kafkaConsumeRunner.getHashMap().get(String.valueOf(kafkaData.getEventId())));

        // update based on event id
        kafkaConsumeRunner.putKafkaDataToHashMap(otherKafkaData);
        assertEquals(otherKafkaData, kafkaConsumeRunner.getHashMap().get(String.valueOf(kafkaData.getEventId())));
    }
}
