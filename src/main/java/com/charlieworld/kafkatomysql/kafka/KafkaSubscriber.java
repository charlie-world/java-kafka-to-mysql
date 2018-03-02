package com.charlieworld.kafkatomysql.kafka;

import com.charlieworld.kafkatomysql.dto.KafkaData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;

public class KafkaSubscriber implements Runnable {

    private List<String> topics;
    private String bootstrapServers;
    private String groupId;
    private Properties props;
    private Consumer<String, String> kafkaConsumer;
    private HashMap<String, KafkaData> hashMap;

    public KafkaSubscriber(List<String> topics, String bootstrapServers, String groupId, HashMap<String, KafkaData> hashMap) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topic list must be not empty list");
        } else if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrap servers must be not null value");
        } else {
            this.topics = topics;
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.hashMap = hashMap;
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

    public KafkaData parseKafkaBody(String body) {
        KafkaData kafkaData = null;
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
            }

            kafkaData = new KafkaData(eventId, eventTimestamp, serviceCode, eventContext);
        } catch (JSONException je) {
            je.getCause();
        }
        return kafkaData;
    }

    public KafkaData putKafkaDataToHashMap(KafkaData kafkaData) {
        return this.hashMap.put(String.valueOf(kafkaData.getEventId()), kafkaData);
    }

    public HashMap<String, KafkaData> getHashMap() {
        return this.hashMap;
    }

    public void setHashMap(Lock mutex, HashMap<String, KafkaData> hashMap) {
        mutex.lock();
        this.hashMap = hashMap;
        mutex.unlock();
    }

    public void run() {
        try {
            kafkaConsumer.subscribe(topics);
            while(!Thread.interrupted()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, String> record : records)
                    putKafkaDataToHashMap(parseKafkaBody(record.value()));

                kafkaConsumer.commitAsync();
            }
        } finally {
            kafkaConsumer.close();
        }
    }

    public void wakeup() {
        kafkaConsumer.wakeup();
    }
}
