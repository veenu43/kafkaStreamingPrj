package com.jpmc.training.avro;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroSenderTest {
    public static void main(String[] args) {
        Properties props=new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer=new KafkaProducer<>(props);
        byte[] array=new byte[1024];
        try {
            FileInputStream fin=new FileInputStream("customer.avsc");
            fin.read(array);
            fin.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Parser parser=new Parser();
        Schema schema=parser.parse(new String(array));
        GenericRecord record=new GenericData.Record(schema);
        record.put("id", 101);
        record.put("name", "Rakesh");
        record.put("email", "rakesh@gmail.com");


        ProducerRecord<String, GenericRecord> rec=new ProducerRecord<String, GenericRecord>("test-avro-topic",
                "rec-1", record);
        producer.send(rec);
        System.out.println("records sent");
        producer.close();
    }
}
