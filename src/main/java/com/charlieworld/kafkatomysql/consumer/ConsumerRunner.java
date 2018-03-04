package com.charlieworld.kafkatomysql.consumer;

import com.charlieworld.kafkatomysql.Runner;
import com.charlieworld.kafkatomysql.dto.KafkaData;

import java.util.HashMap;

public abstract class ConsumerRunner extends Runner {
    public abstract void run();

    public abstract HashMap<String, KafkaData> setHashMap(HashMap<String, KafkaData> hashMap);

    public abstract HashMap<String, KafkaData> getHashMap();
}
