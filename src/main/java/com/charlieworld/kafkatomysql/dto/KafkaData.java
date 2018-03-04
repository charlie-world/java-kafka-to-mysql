package com.charlieworld.kafkatomysql.dto;

import com.charlieworld.kafkatomysql.DbConnector;
import com.charlieworld.kafkatomysql.Runner;

public abstract class KafkaData {

    public abstract Runner mapToRunner(String tableName, String database, DbConnector connector);
}
