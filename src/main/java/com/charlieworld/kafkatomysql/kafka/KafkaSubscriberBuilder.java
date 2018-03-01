package com.charlieworld.kafkatomysql.kafka;

import com.charlieworld.kafkatomysql.dto.KafkaData;
import java.util.HashMap;
import java.util.List;

public class KafkaSubscriberBuilder {

    private List<String> topics;
    private String bootstrapServers;
    private String groupId;
    private HashMap<String, KafkaData> hashMap = null;

    public KafkaSubscriberBuilder() {}

    public KafkaSubscriberBuilder topics(List<String> topics) {
        this.topics = topics;
        return this;
    }

    public KafkaSubscriberBuilder bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public KafkaSubscriberBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public KafkaSubscriberBuilder hashMap(HashMap<String, KafkaData> hashMap) {
        this.hashMap = hashMap;
        return this;
    }

    public KafkaSubscriber build() {
        return new KafkaSubscriber(this.topics, this.bootstrapServers, this.groupId, this.hashMap);
    }
}
