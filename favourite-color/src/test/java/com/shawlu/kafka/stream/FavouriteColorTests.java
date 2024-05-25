package com.shawlu.kafka.stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class FavouriteColorTests {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();

    ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

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
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    public void pushInput(String value) {
        testDriver.pipeInput(recordFactory.create("favourite-colour-input", null, value));
    }

    public ProducerRecord<String, Long> readOutput(){
        return testDriver.readOutput("favourite-colour-output", new StringDeserializer(), new LongDeserializer());
    }

    @Test
    public void makeSureCountsAreCorrect(){
        pushInput("stephane,blue");
        ProducerRecord<String, Long> out1 = readOutput();
        Assert.assertEquals("blue", out1.key());
        Assert.assertEquals(1, out1.value().intValue());

        pushInput("john,green");
        ProducerRecord<String, Long> out2 = readOutput();
        Assert.assertEquals("green", out2.key());
        Assert.assertEquals(1, out2.value().intValue());

        pushInput("stephane,red");
        ProducerRecord<String, Long> out3 = readOutput();
        Assert.assertEquals("blue", out3.key());
        Assert.assertEquals(0, out3.value().intValue());

        ProducerRecord<String, Long> out4 = readOutput();
        Assert.assertEquals("red", out4.key());
        Assert.assertEquals(1, out4.value().intValue());
    }
}
