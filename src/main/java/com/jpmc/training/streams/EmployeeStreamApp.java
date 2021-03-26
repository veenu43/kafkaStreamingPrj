package com.jpmc.training.streams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import com.jpmc.training.domain.Employee;
import com.jpmc.training.serde.EmployeeSerde;
public class EmployeeStreamApp {
    public static void main(String[] args) {

        Properties props=new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,EmployeeSerde.class.getName());
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "employee-stream-app");

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
        String inputTopic="mysql-source-topic-employee";
        String outputTopic="cassandra-sink-topic";
        StreamsBuilder builder=new StreamsBuilder();
        KStream<String, Employee> srcStream=builder.stream(inputTopic);
        KStream<String, Employee> transformedStream=srcStream.mapValues(e->{
            System.out.println("processing employee with id "+e.getEmpid());
            Employee e1=new Employee();
            e1.setEmpid(e.getEmpid());
            e1.setName(e.getName());
            String designation=e.getDesignation();
            e1.setDesignation(designation);
            double salary=10000;
            if(designation.equals("Developer")) {
                salary=20000;
            }
            else if(designation.equals("Accountant")) {
                salary=23000;
            }
            if(designation.equals("Architect")) {
                salary=40000;
            }
            e1.setSalary(salary);
            return e1;
        });
        transformedStream.to(outputTopic);
        return builder.build();
    }

}
