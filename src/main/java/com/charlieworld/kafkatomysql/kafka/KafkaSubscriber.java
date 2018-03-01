package com.charlieworld.kafkatomysql.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;

public class KafkaSubscriber implements Runnable {

    private List<String> topics = null;
    private String bootstrapServers = null;
    private String groupId = "defualt-group";
    private Properties props = null;
    private KafkaConsumer<String, String> kafkaConsumer = null;

    public KafkaSubscriber(List<String> topics, String bootstrapServers, String groupId) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topic list must be not empty list");
        } else if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrap servers must be not null value");
        } else {
            this.topics = topics;
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            kafkaConsumer = new KafkaConsumer<String, String>(props);
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

    public void run() {
        try {
            kafkaConsumer.subscribe(this.topics);
            while(!Thread.interrupted()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, String> record : records)
                    System.out.println(record.value());
            }
        } finally {
            kafkaConsumer.close();
        }
    }

    public void shutdown() {
        kafkaConsumer.wakeup();
    }
}
