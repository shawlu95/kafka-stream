package com.shawlu.kafka.stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class FavouriteColor {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-favourite-colour");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
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
        s.to(Serdes.String(), Serdes.String(), "user-keys-and-colours");
        KTable<String, String> t = builder.table("user-keys-and-colours");

        t.groupBy((k, v) -> new KeyValue<>(v, v))
            .count("counts")
            .to(Serdes.String(), Serdes.Long(), "favourite-colour-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // print topology
        System.out.println(streams);

        // shutdown hook to correctly close the streams app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
