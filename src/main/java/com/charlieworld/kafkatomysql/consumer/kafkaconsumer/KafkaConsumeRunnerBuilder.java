package com.charlieworld.kafkatomysql.consumer.kafkaconsumer;

import java.util.List;
import java.util.concurrent.locks.Lock;

public class KafkaConsumeRunnerBuilder {

    private List<String> topics;
    private String bootstrapServers;
    private String groupId;
    private Lock mutex = null;

    public KafkaConsumeRunnerBuilder() {}

    public KafkaConsumeRunnerBuilder topics(List<String> topics) {
        this.topics = topics;
        return this;
    }

    public KafkaConsumeRunnerBuilder bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public KafkaConsumeRunnerBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public KafkaConsumeRunnerBuilder mutex(Lock mutex) {
        this.mutex = mutex;
        return this;
    }

    public KafkaConsumeRunner build() {
        return new KafkaConsumeRunner(this.topics, this.bootstrapServers, this.groupId, this.mutex);
    }
}
