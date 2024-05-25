package com.shawlu.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class FavouriteColor {
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("favourite-colour-input"); // name of topic

        Set<String> colors = new HashSet<>();
        colors.add("red");
        colors.add("green");
        colors.add("blue");

        KStream<String, String> s = input.filter((k, v) -> v.contains(","))
                .selectKey((k, v) -> v.split(",")[0])
                .mapValues(v -> v.split(",")[1].toLowerCase())
                .filter((k, v) -> colors.contains(v));

        // convert stream to table
        s.to("user-keys-and-colours", Produced.with(Serdes.String(), Serdes.String()));
        KTable<String, String> t = builder.table("user-keys-and-colours");

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        t.groupBy((k, v) -> new KeyValue<>(v, v))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde))
                .toStream()
                .to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-favourite-colour");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        FavouriteColor app = new FavouriteColor();
        KafkaStreams streams = new KafkaStreams(app.createTopology(), config);
        streams.start();

        // print topology
        System.out.println(streams);

        // shutdown hook to correctly close the streams app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
