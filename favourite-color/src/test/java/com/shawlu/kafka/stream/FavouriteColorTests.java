package com.shawlu.kafka.stream;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class FavouriteColorTests {

    TopologyTestDriver testDriver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, Long> outputTopic;

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        FavouriteColor app = new FavouriteColor();
        Topology topology = app.createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic(
                "favourite-colour-input", new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic(
                "favourite-colour-output", new StringDeserializer(), new LongDeserializer());
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void makeSureCountsAreCorrect(){
        inputTopic.pipeInput("stephane,blue");

        Assert.assertEquals(new KeyValue<>("blue", 1L), outputTopic.readKeyValue());

        inputTopic.pipeInput("john,green");
        Assert.assertEquals(new KeyValue<>("green", 1L), outputTopic.readKeyValue());

        inputTopic.pipeInput("stephane,red");
        Assert.assertEquals(new KeyValue<>("blue", 0L), outputTopic.readKeyValue());
        Assert.assertEquals(new KeyValue<>("red", 1L), outputTopic.readKeyValue());

        inputTopic.pipeInput("alice,red");
        Assert.assertEquals(new KeyValue<>("red", 2L), outputTopic.readKeyValue());
    }
}
