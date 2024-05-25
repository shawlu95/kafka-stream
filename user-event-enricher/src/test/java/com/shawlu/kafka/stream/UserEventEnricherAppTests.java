package com.shawlu.kafka.stream;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class UserEventEnricherAppTests {
    TopologyTestDriver testDriver;
    TestInputTopic<String, String> userTopic;
    TestInputTopic<String, String> purchaseTopic;
    TestOutputTopic<String, String> innerJoinTopic;
    TestOutputTopic<String, String> leftJoinTopic;

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        UserEventEnricherApp app = new UserEventEnricherApp();
        Topology topology = app.createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        userTopic = testDriver.createInputTopic(
                "user-table", new StringSerializer(), new StringSerializer());
        purchaseTopic = testDriver.createInputTopic(
                "user-purchases", new StringSerializer(), new StringSerializer());
        innerJoinTopic = testDriver.createOutputTopic(
                "user-purchases-enriched-inner-join", new StringDeserializer(), new StringDeserializer());
        leftJoinTopic = testDriver.createOutputTopic(
                "user-purchases-enriched-left-join", new StringDeserializer(), new StringDeserializer());
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void testGoodJoin() {
        userTopic.pipeInput("john", "First=John,Last=Doe,Email=john.doe@gmail.com");
        purchaseTopic.pipeInput("john", "Apples and Bananas (1)");

        KeyValue<String, String> inner = innerJoinTopic.readKeyValue();
        Assert.assertEquals("john", inner.key);
        Assert.assertEquals("Purchase=Apples and Bananas (1),UserInfo=[First=John,Last=Doe,Email=john.doe@gmail.com]", inner.value);

        KeyValue<String, String> left = leftJoinTopic.readKeyValue();
        Assert.assertEquals("john", left.key);
        Assert.assertEquals("Purchase=Apples and Bananas (1),UserInfo=[First=John,Last=Doe,Email=john.doe@gmail.com]", left.value);
    }

    @Test
    public void testNonExistingUser() {
        purchaseTopic.pipeInput("bob", "Kafka Udemy Course (2)");

//        Assert.assertNull(innerJoinTopic.readKeyValue());

        KeyValue<String, String> left = leftJoinTopic.readKeyValue();
        Assert.assertEquals("bob", left.key);
        Assert.assertEquals("Purchase=Kafka Udemy Course (2),UserInfo=null", left.value);
    }

    @Test
    public void testUpdateUser() {
        userTopic.pipeInput("john", "First=John,Last=Doe,Email=john.doe@gmail.com");
        userTopic.pipeInput("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com");
        purchaseTopic.pipeInput("john", "Oranges (3)");

        KeyValue<String, String> inner = innerJoinTopic.readKeyValue();
        Assert.assertEquals("john", inner.key);
        Assert.assertEquals("Purchase=Oranges (3),UserInfo=[First=Johnny,Last=Doe,Email=johnny.doe@gmail.com]", inner.value);

        KeyValue<String, String> left = leftJoinTopic.readKeyValue();
        Assert.assertEquals("john", left.key);
        Assert.assertEquals("Purchase=Oranges (3),UserInfo=[First=Johnny,Last=Doe,Email=johnny.doe@gmail.com]", left.value);
    }

    @Test
    public void purchaseBeforeUserExists() {
        // first purchase before user creation
        purchaseTopic.pipeInput("stephane", "Computer (4)");
        userTopic.pipeInput("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph");

//        Assert.assertNull(innerJoinTopic.readKeyValue());

        KeyValue<String, String> left = leftJoinTopic.readKeyValue();
        Assert.assertEquals("stephane", left.key);
        Assert.assertEquals("Purchase=Computer (4),UserInfo=null", left.value);

        // second purchase
        purchaseTopic.pipeInput("stephane", "Books (4)");

        KeyValue<String, String> inner = innerJoinTopic.readKeyValue();
        Assert.assertEquals("stephane", inner.key);
        Assert.assertEquals("Purchase=Books (4),UserInfo=[First=Stephane,Last=Maarek,GitHub=simplesteph]", inner.value);

        KeyValue<String, String> left2 = leftJoinTopic.readKeyValue();
        Assert.assertEquals("stephane", left2.key);
        Assert.assertEquals("Purchase=Books (4),UserInfo=[First=Stephane,Last=Maarek,GitHub=simplesteph]", left2.value);
    }

    @Test
    public void testDeleteUser() {
        userTopic.pipeInput("alice", "First=Alice");
        userTopic.pipeInput(new TestRecord<>("alice", null));
        purchaseTopic.pipeInput("alice", "Apache Kafka Series (5)");

//        Assert.assertNull(innerJoinTopic.readKeyValue());

        KeyValue<String, String> left = leftJoinTopic.readKeyValue();
        Assert.assertEquals("alice", left.key);
        Assert.assertEquals("Purchase=Apache Kafka Series (5),UserInfo=null", left.value);
    }
}