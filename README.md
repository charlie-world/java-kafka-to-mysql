# java-kafka-to-mysql

## Architecture



## How to use

### Step 1: Environment

**Properties file must be exist**

Example of `config.properties`
```
db.username=user
db.password=password
db.table_name=event
db.database=kafka
db.host=localhost
db.port=3306

consumer.group_id=1
consumer.bootstrap_servers=localhost:9092
consumer.topics=test

interval.min=1         // interval time is 1 minutes

maxThreadPoolSize=10   // This value is not always required
```


### Step 2: Codes

#### Main Program

```
public class KafkaToMysql {
    public static void main(String[] args) {
        String PROPERTIES_PATH = "PATH_CONFIG_PROPERTIES_FILE_IN";
        Service service = new KafkaToMysqlService(PROPERTIES_PATH);
        service.start();
    }
}
```

### Example

#### You can run the simple Integration test

`docker-compose up` must be started before run Integration test

``src/test/java/com/charlieworld/kafkatomysql/IntegrationSpec.java`` 