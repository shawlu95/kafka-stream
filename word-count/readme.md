```bash
# optional: open a shell - zookeeper is at localhost:2181
bin/zookeeper-server-start.sh config/zookeeper.properties

# open another shell - kafka is at localhost:9092
bin/kafka-server-start.sh config/server.properties

# create input topic
bin/kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic word-count-input --bootstrap-server localhost:9092

# create output topic
bin/kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic word-count-output --bootstrap-server localhost:9092

# start a kafka producer
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic word-count-input
# enter
kafka streams udemy
kafka data processing
kafka streams course
# exit

# verify the data has been written
bin/kafka-console-consumer.sh --topic word-count-input --from-beginning --bootstrap-server localhost:9092

# start a consumer on the output topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic word-count-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

## Run Fat Jar

```bash
mvn clean package

java -jar target/word-count-1.0-SNAPSHOT-jar-with-dependencies.jar
```