package com.charlieworld.kafkatomysql;

import com.charlieworld.kafkatomysql.runner.kafka.KafkaProducerRunner;
import com.charlieworld.kafkatomysql.scheduler.MainScheduler;
import com.charlieworld.kafkatomysql.scheduler.Scheduler;

public class IntegrationSpec {
    public static void main(String[] args) {
        String topic = "test";
        KafkaProducerRunner kafkaProducerRunner = new KafkaProducerRunner(topic);
        Scheduler scheduler = new MainScheduler();
        kafkaProducerRunner.start();
        scheduler.runMain();
    }
}
