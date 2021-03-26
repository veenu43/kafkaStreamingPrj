package com.jpmc.training.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApp {

    public static void main(String[] args) {

        Properties props=new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "sample-stream-app");

        Topology topology=buildTopology();

        KafkaStreams streams=new KafkaStreams(topology, props);

        streams.start();
        System.out.println("streaming started");

        try {
            Thread.sleep(5*60*1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    static Topology buildTopology()
    {
        String inputTopic="source-connect-topic";
        String outputTopic="sink-connect-topic";
        StreamsBuilder builder=new StreamsBuilder();
        KStream<String, String> srcStream=builder.stream(inputTopic);
        KStream<String, String> transformedStream=srcStream.mapValues(v->{
            System.out.println("processing "+v);
            return v.toUpperCase();
        });
        transformedStream.to("sink-connect-topic");
        return builder.build();
    }
}
