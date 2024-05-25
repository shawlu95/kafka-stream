package com.shawlu.kafka.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.common.serialization.*;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceExactlyOnceAppTests {
    TopologyTestDriver testDriver;
    TestInputTopic<String, JsonNode> inputTopic;
    TestOutputTopic<String, JsonNode> outputTopic;

    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        BankBalanceExactlyOnceApp app = new BankBalanceExactlyOnceApp();
        Topology topology = app.createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic(
                "bank-transactions", new StringSerializer(), jsonSerializer);
        outputTopic = testDriver.createOutputTopic(
                "bank-balance-exactly-once", new StringDeserializer(), jsonDeserializer);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void testBalance(){
        // deposit 100
        JsonNode txn = JsonNodeFactory.instance.objectNode()
                .put("name", "shaw")
                .put("amount", 100)
                .put("time", Instant.now().toString());
        inputTopic.pipeInput("shaw", txn);
        KeyValue<String, JsonNode> state = outputTopic.readKeyValue();
        Assert.assertEquals("shaw", state.key);
        Assert.assertEquals(1, state.value.get("count").asInt());
        Assert.assertEquals(100, state.value.get("balance").asInt());

        // withdraw 80
        JsonNode txn2 = JsonNodeFactory.instance.objectNode()
                .put("name", "shaw")
                .put("amount", -80)
                .put("time", Instant.now().toString());
        inputTopic.pipeInput("shaw", txn2);
        KeyValue<String, JsonNode> state2 = outputTopic.readKeyValue();
        Assert.assertEquals("shaw", state2.key);
        Assert.assertEquals(2, state2.value.get("count").asInt());
        Assert.assertEquals(20, state2.value.get("balance").asInt());
    }
}
