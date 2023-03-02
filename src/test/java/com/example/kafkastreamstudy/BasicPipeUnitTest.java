package com.example.kafkastreamstudy;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BasicPipeUnitTest {
    @Test
    void pipeTest(){
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wctest");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final String inputName = "streams-input";
        final String outputName = "streams-output";

        builder.<String, String>stream(inputName)
                .to(outputName, Produced.with(Serdes.String(), Serdes.String()));

        final Topology topology = builder.build();

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
        final TestInputTopic<String, String> inputTopic =
                testDriver.createInputTopic(inputName, Serdes.String().serializer(), Serdes.String().serializer());

        final TestOutputTopic<String, String> outputTopic =
                testDriver.createOutputTopic(outputName, Serdes.String().deserializer(), Serdes.String().deserializer());

        final List<String> testInput = new ArrayList<>();
        testInput.add("BC CD");
        testInput.add("BC");

        List<String> expectedOutput = List.of("BC CD", "BC");

        testInput.forEach(x -> inputTopic.pipeInput(x, x));
        List<String> actualOutput = outputTopic.readValuesToList();

        assertEquals(expectedOutput, actualOutput);

    }
}
