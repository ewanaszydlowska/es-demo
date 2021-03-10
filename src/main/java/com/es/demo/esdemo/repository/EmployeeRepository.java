package com.es.demo.esdemo.repository;

import com.es.demo.esdemo.model.Employee;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface EmployeeRepository extends ElasticsearchRepository<Employee, String> {

    List<Employee> findByLastName(String lastName);
    List<Employee> findByLastNameAndFirstName(String lastName, String firstName);
}
