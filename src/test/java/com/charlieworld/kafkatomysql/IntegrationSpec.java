package com.charlieworld.kafkatomysql;

import com.charlieworld.kafkatomysql.service.KafkaToMysqlService;

public class IntegrationSpec {
    public static void main(String[] args) {

        /**
         *  Integration Test
         *  topic must be equal as `topic` value in properties file.
         * */

        String topic = "test";
        String PROPERTIES_PATH = ClassLoader.class.getResource("/config.properties").getPath();

        SampleKafkaProducer sampleKafkaProducer = new SampleKafkaProducer(topic);
        Service service = new KafkaToMysqlService(PROPERTIES_PATH);
        sampleKafkaProducer.start();
        service.start();
    }
}
