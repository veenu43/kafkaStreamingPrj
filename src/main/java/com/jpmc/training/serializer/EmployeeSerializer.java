package com.jpmc.training.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.training.domain.Employee;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;



public class EmployeeSerializer  implements Serializer<Employee> {
    private ObjectMapper mapper=new ObjectMapper();
    @Override
    public byte[] serialize(String topic, Employee data) {
        // TODO Auto-generated method stub
        byte[] array=null;

        try {
            array=mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return array;
    }
}
