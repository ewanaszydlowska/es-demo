package com.es.demo.esdemo.service;

import com.es.demo.esdemo.model.Employee;
import com.es.demo.esdemo.repository.EmployeeRepository;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.SearchScrollHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class EmployeeService {

    private final RestHighLevelClient client;
    private final EmployeeRepository repo;
    private ElasticsearchRestTemplate elasticsearchTemplate;

    public EmployeeService(RestHighLevelClient client, EmployeeRepository repo, ElasticsearchRestTemplate elasticsearchTemplate) {
        this.client = client;
        this.repo = repo;
        this.elasticsearchTemplate = elasticsearchTemplate;
    }

    public Employee saveEmployee(Employee employee) {
        return repo.save(employee);
    }

    public List<Employee> getEmployeesByName(String lastName, String firstName) {
        List<Employee> employees;
        if(firstName != null) {
             employees = repo.findByLastNameAndFirstName(lastName, firstName);
        } else {
            employees = repo. findByLastName(lastName);
        }
        return employees;
    }

    /*{
        "query": {
            "match": {
                "interests": "illusion, tv wtaching"
            }
        }
    }*/
    /*Spring data way*/
    public SearchHits<Employee> getByInterests(List<String> interests) {
        String interestsStr = StringUtils.join(interests, ",");
        Query query = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.matchQuery("interests", interestsStr))
                .withPageable(PageRequest.of(0, 10))
                .build();

        SearchHits<Employee> employees = elasticsearchTemplate.search(query, Employee.class, IndexCoordinates.of("employee"));
        return employees;
    }
    
    /*Java API way*/
    public SearchHit[] getByAddress(String address) throws IOException {
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("address", address);
        SearchSourceBuilder builder = new SearchSourceBuilder().query(queryBuilder).fetchSource(new String[]{"firstName", "lastName", "address"}, new String[0]);

        SearchRequest searchRequest = new SearchRequest().indices("employee").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return response.getHits().getHits();
    }

    /*{
        "aggs": {
            "avg_age": {
                "avg": {
                    "field": "age"
                }
            }
        }
    }*/
    public Double getAverageAge() throws IOException {
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg("avg_age").field("age");
        SearchSourceBuilder builder = new SearchSourceBuilder().aggregation(avgAgg).size(0);

        SearchRequest searchRequest = new SearchRequest().indices("employee").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        ParsedAvg results = response.getAggregations().get("avg_age");
        double avg = results.getValue();
        return avg;
    }

    /*{
        "aggs": {
            "empl_gender": {
                "terms": {
                    "field": "gender.keyword"
                },
                "aggs": {
                    "salary_buckets": {
                        "histogram": {
                            "field": "salary",
                                    "interval": 10000
                        }
                    }
                }
            }
        },
        "size": 0
    }*/
    public String getSalariesHistogramByGender() throws IOException {
        TermsAggregationBuilder genderAgg = AggregationBuilders.terms("empl_gender").field("gender.keyword")
                .subAggregation(AggregationBuilders.histogram("salary_buckets").field("salary").interval(10000));
        SearchSourceBuilder builder = new SearchSourceBuilder().aggregation(genderAgg).size(0);

        SearchRequest searchRequest = new SearchRequest().indices("employee").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        Terms results = (ParsedStringTerms) response.getAggregations().get("empl_gender");
        List<ParsedStringTerms.ParsedBucket> buckets = (List<ParsedStringTerms.ParsedBucket>) results.getBuckets();

        return response.toString();
    }

    /*{
        "aggs": {
        "empl_gender": {
            "terms": {
                "field": "gender.keyword"
            },
            "aggs": {
                "avg_salary": {
                    "avg": {
                        "field": "salary"
                    }
                }
            }
        }
    },
        "size": 0
    }*/
    public String getAvgSalaryByGender() throws IOException {
        TermsAggregationBuilder terms = AggregationBuilders.terms("empl_gender").field("gender.keyword")
                .subAggregation(AggregationBuilders.avg("ang_salary").field("salary"));
        SearchSourceBuilder builder = new SearchSourceBuilder().aggregation(terms).size(0);

        SearchRequest searchRequest = new SearchRequest().indices("employee").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        return response.toString();
    }

    /*{
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                { "term": { "age": 59 } },
                                { "term": { "gender.keyword": "Female" } }
                            ]
                        }
                    },
                    {
                        "bool": {
                            "must": [
                                { "term": { "age": 64 } },
                                { "term": { "gender.keyword": "Male" } }
                            ]
                        }
                    }
                ]
            }
        }
    }*/
    public long getNumberOfRetiringWithinYear() throws IOException {
        BoolQueryBuilder femaleQuery = QueryBuilders.boolQuery()
                .must(new TermQueryBuilder("age", 59))
                .must(new TermQueryBuilder("gender.keyword", "Female"));

        BoolQueryBuilder maleQuery = QueryBuilders.boolQuery()
                .must(new TermQueryBuilder("age", 64))
                .must(new TermQueryBuilder("gender.keyword", "Male"));

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .should(femaleQuery)
                .should(maleQuery);

        SearchSourceBuilder builder = new SearchSourceBuilder().query(queryBuilder);

        SearchRequest searchRequest = new SearchRequest().indices("employee").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        return response.getHits().getTotalHits().value;
    }

    public void reindex() {
        IndexCoordinates index = IndexCoordinates.of("employee");

        Query searchQuery = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.matchAllQuery())
                .withFields("message")
                .withPageable(PageRequest.of(0, 10))
                .build();

        SearchScrollHits<Employee> scroll = elasticsearchTemplate.searchScrollStart(1000, searchQuery, Employee.class, index);

        String scrollId = scroll.getScrollId();
        List<org.springframework.data.elasticsearch.core.SearchHit<Employee>> sampleEntities = new ArrayList<>();
        while (scroll.hasSearchHits()) {
            sampleEntities.addAll(scroll.getSearchHits());
            scrollId = scroll.getScrollId();
            scroll = elasticsearchTemplate.searchScrollContinue(scrollId, 1000, Employee.class, index);
        }
        elasticsearchTemplate.searchScrollClear(Collections.singletonList(scrollId));
    }
}
