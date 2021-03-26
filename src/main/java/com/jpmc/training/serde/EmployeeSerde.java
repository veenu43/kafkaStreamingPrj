package com.jpmc.training.serde;

import com.jpmc.training.deserializer.EmployeeDeserializer;
import com.jpmc.training.domain.Employee;
import com.jpmc.training.serializer.EmployeeSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class EmployeeSerde  implements Serde<Employee> {

    @Override
    public Serializer<Employee> serializer() {
        // TODO Auto-generated method stub
        return new EmployeeSerializer();
    }

    @Override
    public Deserializer<Employee> deserializer() {
        // TODO Auto-generated method stub
        return new EmployeeDeserializer();
    }

}
