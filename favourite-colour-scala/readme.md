## favourite-colour-scala

How to set up the project
- File > Project Structure > select Java 1.8
- Run > Edit Configurations > Add sbt task > enter command `~run`, and assign a `$task_name`
- Run > `$task_name` defined in previous step
- Go to scala object, add Scala SDK, download version 2.12.3
- Click "sbt" on the right-hand-side of the screen and click refresh button, to get rid of the warnings

```bash

# optional: open a shell - zookeeper is at localhost:2181
bin/zookeeper-server-start.sh config/zookeeper.properties

# open another shell - kafka is at localhost:9092
bin/kafka-server-start.sh config/server.properties

# create input topic with one partition to get full ordering
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic favourite-colour-input-scala

# create intermediary log compacted topic
bin/kafka-topics.sh --create --zookeeper localhost:2181  --replication-factor 1 --partitions 1 --topic user-keys-and-colours-scala --config cleanup.policy=compact

# create output log compacted topic
bin/kafka-topics.sh --create --zookeeper localhost:2181  --replication-factor 1 --partitions 1 --topic favourite-colour-output-scala --config cleanup.policy=compact


# launch a Kafka consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic favourite-colour-output-scala \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# launch the streams application

# then produce data to it
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic favourite-colour-input-scala
#
stephane,blue
john,green
stephane,red
alice,red


# list all topics that we have in Kafka (so we can observe the internal topics)
bin/kafka-topics.sh --list --zookeeper localhost:2181