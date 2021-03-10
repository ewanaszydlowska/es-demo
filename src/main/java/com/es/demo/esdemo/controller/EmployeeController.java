package com.es.demo.esdemo.controller;

import com.es.demo.esdemo.model.Employee;
import com.es.demo.esdemo.service.EmployeeService;
import org.elasticsearch.search.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/employee")
public class EmployeeController {

    private final EmployeeService employeeService;

    public EmployeeController(EmployeeService employeeService) {
        this.employeeService = employeeService;
    }

    @PostMapping("/")
    public Employee saveEmployee(@RequestBody Employee employee) {
        return employeeService.saveEmployee(employee);
    }

    @GetMapping("/name/search")
    public List<Employee> getEmployeesByName(@RequestParam String lastName, @RequestParam(required = false) String firstName) {
        List<Employee> empls = employeeService.getEmployeesByName(lastName, firstName);
        return empls;
    }

    @PostMapping("/interest/search")
    public SearchHits<Employee> getByInterests(@RequestBody List<String> interests) {
        return employeeService.getByInterests(interests);
    }

    @GetMapping("/address/search/{address}")
    public SearchHit[] getByAddress(@PathVariable String address) throws IOException {
        return employeeService.getByAddress(address);
    }

    @GetMapping("/age/avg")
    public Double getAverageAge() throws IOException {
        return employeeService.getAverageAge();
    }

    @GetMapping("/gender/salaries/histogram")
    public String getSalariesHistogramByGender() throws IOException {
        return employeeService.getSalariesHistogramByGender();
    }

    @GetMapping("/gender/salaries/avg")
    public String getAvgSalaryByGender() throws IOException {
        return employeeService.getAvgSalaryByGender();
    }

    @GetMapping("/age/retirement-within-year")
    public long getNumberOfRetiringWithinYear() throws IOException {
        return employeeService.getNumberOfRetiringWithinYear();
    }

    @GetMapping("/reindex")
    public void reindex() {
        employeeService.reindex();
    }

}
