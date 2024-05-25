package com.shawlu.kafka.stream;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class WordCountAppTest {

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

        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic(
                "word-count-input", new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic(
                "word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka Streams";
        inputTopic.pipeInput(firstExample);
        Assert.assertEquals(new KeyValue<>("testing", 1L), outputTopic.readKeyValue());
        Assert.assertEquals(new KeyValue<>("kafka", 1L), outputTopic.readKeyValue());
        Assert.assertEquals(new KeyValue<>("streams", 1L), outputTopic.readKeyValue());

        String secondExample = "testing Kafka again";
        inputTopic.pipeInput(secondExample);
        Assert.assertEquals(new KeyValue<>("testing", 2L), outputTopic.readKeyValue());
        Assert.assertEquals(new KeyValue<>("kafka", 2L), outputTopic.readKeyValue());
        Assert.assertEquals(new KeyValue<>("again", 1L), outputTopic.readKeyValue());
    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka Kafka";
        inputTopic.pipeInput(upperCaseString);
        Assert.assertEquals(new KeyValue<>("kafka", 1L), outputTopic.readKeyValue());
        Assert.assertEquals(new KeyValue<>("kafka", 2L), outputTopic.readKeyValue());
        Assert.assertEquals(new KeyValue<>("kafka", 3L), outputTopic.readKeyValue());
    }
}