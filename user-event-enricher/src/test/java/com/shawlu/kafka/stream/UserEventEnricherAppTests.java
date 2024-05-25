package com.shawlu.kafka.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class UserEventEnricherAppTests {
    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();

    ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

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
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    public void pushUser(String name, String profile) {
        testDriver.pipeInput(recordFactory.create("user-table", name, profile));
    }

    public void pushPurchase(String name, String purchase) {
        testDriver.pipeInput(recordFactory.create("user-purchases", name, purchase));
    }

    public ProducerRecord<String, String> readInnerJoin(){
        return testDriver.readOutput("user-purchases-enriched-inner-join", stringDeserializer, stringDeserializer);
    }

    public ProducerRecord<String, String> readLeftJoin(){
        return testDriver.readOutput("user-purchases-enriched-left-join", stringDeserializer, stringDeserializer);
    }

    @Test
    public void testGoodJoin() {
        pushUser("john", "First=John,Last=Doe,Email=john.doe@gmail.com");
        pushPurchase("john", "Apples and Bananas (1)");

        ProducerRecord<String, String> inner = readInnerJoin();
        Assert.assertEquals("john", inner.key());
        Assert.assertEquals("Purchase=Apples and Bananas (1),UserInfo=[First=John,Last=Doe,Email=john.doe@gmail.com]", inner.value());

        ProducerRecord<String, String> left = readLeftJoin();
        Assert.assertEquals("john", left.key());
        Assert.assertEquals("Purchase=Apples and Bananas (1),UserInfo=[First=John,Last=Doe,Email=john.doe@gmail.com]", left.value());
    }

    @Test
    public void testNonExistingUser() {
        pushPurchase("bob", "Kafka Udemy Course (2)");

        ProducerRecord<String, String> inner = readInnerJoin();
        Assert.assertNull(inner);

        ProducerRecord<String, String> left = readLeftJoin();
        Assert.assertEquals("bob", left.key());
        Assert.assertEquals("Purchase=Kafka Udemy Course (2),UserInfo=null", left.value());
    }

    @Test
    public void testUpdateUser() {
        pushUser("john", "First=John,Last=Doe,Email=john.doe@gmail.com");
        pushUser("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com");
        pushPurchase("john", "Oranges (3)");

        ProducerRecord<String, String> inner = readInnerJoin();
        Assert.assertEquals("john", inner.key());
        Assert.assertEquals("Purchase=Oranges (3),UserInfo=[First=Johnny,Last=Doe,Email=johnny.doe@gmail.com]", inner.value());

        ProducerRecord<String, String> left = readLeftJoin();
        Assert.assertEquals("john", left.key());
        Assert.assertEquals("Purchase=Oranges (3),UserInfo=[First=Johnny,Last=Doe,Email=johnny.doe@gmail.com]", left.value());
    }

    @Test
    public void purchaseBeforeUserExists() {
        // first purchase before user creation
        pushPurchase("stephane", "Computer (4)");
        pushUser("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph");

        Assert.assertNull(readInnerJoin());

        ProducerRecord<String, String> left = readLeftJoin();
        Assert.assertEquals("stephane", left.key());
        Assert.assertEquals("Purchase=Computer (4),UserInfo=null", left.value());

        // second purchase
        pushPurchase("stephane", "Books (4)");

        ProducerRecord<String, String> inner = readInnerJoin();
        Assert.assertEquals("stephane", inner.key());
        Assert.assertEquals("Purchase=Books (4),UserInfo=[First=Stephane,Last=Maarek,GitHub=simplesteph]", inner.value());

        ProducerRecord<String, String> left2 = readLeftJoin();
        Assert.assertEquals("stephane", left2.key());
        Assert.assertEquals("Purchase=Books (4),UserInfo=[First=Stephane,Last=Maarek,GitHub=simplesteph]", left2.value());
    }

    @Test
    public void testDeleteUser() {
        pushUser("alice", "First=Alice");
        pushUser("alice", null);
        pushPurchase("alice", "Apache Kafka Series (5)");

        Assert.assertNull(readInnerJoin());

        ProducerRecord<String, String> left = readLeftJoin();
        Assert.assertEquals("alice", left.key());
        Assert.assertEquals("Purchase=Apache Kafka Series (5),UserInfo=null", left.value());
    }
}