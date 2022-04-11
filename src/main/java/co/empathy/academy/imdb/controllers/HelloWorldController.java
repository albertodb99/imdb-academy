package co.empathy.academy.imdb.controllers;
import client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import co.empathy.academy.imdb.utils.TsvReader;

import java.io.File;

@RestController
public class HelloWorldController {

    ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    @GetMapping("/greet/{name}")
    public String greet(@PathVariable String name){
        TsvReader.indexFile("title.basics.tsv", name);
        return "Hello " + name;
    }


}
