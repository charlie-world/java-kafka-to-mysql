package com.charlieworld.kafkatomysql;

import com.charlieworld.kafkatomysql.runner.kafka.KafkaProducerRunner;
import com.charlieworld.kafkatomysql.scheduler.MainScheduler;
import com.charlieworld.kafkatomysql.scheduler.Scheduler;

import java.io.IOException;

public class IntegrationSpec {
    public static void main(String[] args) {

//        try {
//            Process proc = Runtime.getRuntime().exec("docker-compose up");
//            proc.wait();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        String topic = "test";
        KafkaProducerRunner kafkaProducerRunner = new KafkaProducerRunner(topic);
        Scheduler scheduler = new MainScheduler();
        kafkaProducerRunner.start();
        scheduler.runMain();
    }
}
