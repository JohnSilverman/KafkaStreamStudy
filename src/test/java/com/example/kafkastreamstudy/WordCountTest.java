package com.example.kafkastreamstudy;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class WordCountTest {

    @BeforeEach
    void setUp() {

    }

    @AfterEach
    void tearDown() {

    }

    @Test
    void wordCountTest() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wctest");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final String inputName = "streams-input";
        final String outputName = "streams-output";

        builder.<String, String>stream(inputName)
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                .toStream()
                .to(outputName, Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
        final TestInputTopic<String, String> inputTopic =
                testDriver.createInputTopic(inputName, Serdes.String().serializer(), Serdes.String().serializer());

        final TestOutputTopic<String, Long> outputTopic =
                testDriver.createOutputTopic(outputName, Serdes.String().deserializer(), Serdes.Long().deserializer());

        final List<String> testInput = new ArrayList<>();
        testInput.add("AB BC CD");
        testInput.add("BC CD");
        testInput.add("BC");

        List<Long> expectedOutput = List.of(1L, 1L, 1L, 2L, 2L, 3L);

        testInput.forEach(x -> inputTopic.pipeInput(x, x));
        List<Long> actualOutput = outputTopic.readValuesToList();

        assertEquals(expectedOutput, actualOutput);


    }

}
