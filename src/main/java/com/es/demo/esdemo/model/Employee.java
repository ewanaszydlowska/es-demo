package com.es.demo.esdemo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.LocalDate;

@Document(indexName = "employee")
@Builder
@Data
@AllArgsConstructor
public class Employee {

    @Id
    private String id;
    private String firstName;

    @Field(fielddata = true)
    private String lastName;
    private String designation;
    private Integer salary;

    @Field(type = FieldType.Date, format = DateFormat.year_month_day)
    private LocalDate dateOfJoining;
    private String address;

    @Field(fielddata = true)
    private String gender;
    private Integer age;
    private String maritalStatus;
    private String interests;
}
