package com.charlieworld.kafkatomysql;

import com.charlieworld.kafkatomysql.scheduler.MainScheduler;
import com.charlieworld.kafkatomysql.scheduler.Scheduler;

public class IntegrationSpec {
    public static void main(String[] args) {

        String topic = "test";
        String PROPERTIES_PATH = ClassLoader.class.getResource("/config.properties").getPath();

        KafkaProducerRunner kafkaProducerRunner = new KafkaProducerRunner(topic);
        Scheduler scheduler = new MainScheduler(PROPERTIES_PATH);
        kafkaProducerRunner.start();
        scheduler.runMain();
    }
}
