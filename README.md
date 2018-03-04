# java-kafka-to-mysql

## How to use

### Step 1: Environment

####Properties file must be exist

Example of `config.properties`
```
username=user
password=password
table_name=event
database=kafka
host=localhost
port=3306
group_id=1
bootstrap_servers=localhost:9092
topics=test
interval_time=1
```

(Everything in this example, are required)

### Step 2: Codes

#### Main Program

```
public class KafkaToMysql {
    public static void main(String[] args) {
        String PROPERTIES_PATH = ClassLoader.class.getResource("/config.properties").getPath();
        Service service = new KafkaToMysqlService(PROPERTIES_PATH);
        service.start();
    }
}
```

#### You can run the simple Integration test

`docker-compose up` must be started before run Integration test

``src/test/java/com/charlieworld/kafkatomysql/IntegrationSpec.java`` 