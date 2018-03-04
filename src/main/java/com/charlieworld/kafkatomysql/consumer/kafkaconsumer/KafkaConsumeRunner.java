package com.charlieworld.kafkatomysql.consumer.kafkaconsumer;

import com.charlieworld.kafkatomysql.consumer.ConsumerRunner;
import com.charlieworld.kafkatomysql.dto.KafkaData;
import com.charlieworld.kafkatomysql.dto.kafkadata.EventData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;

public class KafkaConsumeRunner extends ConsumerRunner {

    private List<String> topics;
    private String bootstrapServers;
    private String groupId;
    private Properties props;
    private Consumer<String, String> kafkaConsumer;
    private HashMap<String, KafkaData> hashMap = new HashMap<String, KafkaData>();

    private Lock mutex = null;

    public KafkaConsumeRunner(List<String> topics, String bootstrapServers, String groupId, Lock mutex) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topic list must not be empty list");
        } else if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrap servers must not be null value");
        } else {
            this.topics = topics;
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.mutex = mutex;
            props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            this.kafkaConsumer = new KafkaConsumer<String, String>(props);
        }
    }

    public List<String> getTopics() {
        return this.topics;
    }

    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    public String getGroupId() {
        return this.groupId;
    }

    public Properties getProps() {
        return this.props;
    }

    public HashMap<String, KafkaData> setHashMap(HashMap<String, KafkaData> hashMap) {
        mutex.lock();
        this.hashMap = hashMap;
        mutex.unlock();
        return this.hashMap;
    }

    public HashMap<String, KafkaData> getHashMap() { return this.hashMap; }

    public EventData parseKafkaBody(String body) {
        EventData eventData = null;
        try {
            JSONObject jsonObject = new JSONObject(body);
            long eventId = jsonObject.getLong("event_id");
            String eventTimestamp = jsonObject.getString("event_timestamp");

            String serviceCode = null;
            String eventContext = null;

            try {
                serviceCode = jsonObject.getString("service_code");
                eventContext = jsonObject.getString("event_context");
            } catch (JSONException je) {
                // nothing
                je.printStackTrace();
            }

            eventData = new EventData(eventId, eventTimestamp, serviceCode, eventContext);
        } catch (JSONException je) {
            je.printStackTrace();
        }
        return eventData;
    }

    public EventData putKafkaDataToHashMap(EventData eventData) {
        mutex.lock();
        this.hashMap.put(String.valueOf(eventData.getEventId()), eventData);
        mutex.unlock();
        return eventData;
    }

    public void run() {
        try {
            kafkaConsumer.subscribe(topics);
            while(!Thread.interrupted()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    putKafkaDataToHashMap(parseKafkaBody(record.value()));
                }
                kafkaConsumer.commitAsync();
            }
        } catch (InterruptException ie) {
            ie.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

    public void wakeup() {
        kafkaConsumer.wakeup();
    }
}
