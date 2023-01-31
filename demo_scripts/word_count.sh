#!/bin/bash

### LINUX / MAC OS X ONLY

# optional: open a shell - zookeeper is at localhost:2181
# bin/zookeeper-server-start.sh config/zookeeper.properties

# open another shell - kafka is at localhost:9092
bin/kafka-server-start.sh config/server.properties

# create input topic
bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic streams-plaintext-input --bootstrap-server localhost:9092

# create output topic
bin/kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic streams-wordcount-output --bootstrap-server localhost:9092

# start a kafka producer
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-plaintext-input
# enter
kafka streams udemy
kafka data processing
kafka streams course
# exit

# verify the data has been written
bin/kafka-console-consumer.sh --topic streams-plaintext-input --from-beginning --bootstrap-server localhost:9092

# start a consumer on the output topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# start the streams application
bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo

# verify the data has been written to the output topic!
