package com.charlieworld.kafkatomysql;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerRunner extends Thread {

    private Properties properties = new Properties();
    private KafkaProducer<Integer, String> kafkaProducer = null;
    private String topic = null;

    public KafkaProducerRunner(String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("topic must not be null value");
        } else {
            this.topic = topic;
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("client.id", "DemoProducer");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducer = new KafkaProducer<Integer, String>(properties);
        }
    }

    public String createMsg(int msgId) {
        return String.format(
                "{event_id: %d, event_timestamp: '%s', service_code: '%s', event_context: '%s'}",
                msgId,
                new Date(System.currentTimeMillis()).toString(),
                "SERVICE_CODE",
                "EVENT_CONTEXT"
        );
    }

    public void run() {
        int msgId = 1;
        while(true) {
            try {
                Thread.sleep(1000);
                kafkaProducer.send(new ProducerRecord<Integer, String>(topic,
                        msgId,
                        createMsg(msgId))).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            msgId += 1;
        }
    }
}
