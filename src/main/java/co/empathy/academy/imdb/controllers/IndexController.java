package co.empathy.academy.imdb.controllers;

import client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.imdb.utils.TsvReader;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
public class IndexController {

    ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    @GetMapping("/_cat/indices")
    public Map<String, IndexState> getIndexes(){
        try {
            return client.indices().get( c -> c.index("*")).result();
        } catch (IOException    e) {
            e.printStackTrace();
        }
        return new HashMap<>();
    }

    @DeleteMapping("{indexName}")
    public void deleteIndex(@PathVariable String indexName){
        try {
            client.indices()
                    .delete(b -> b.index(indexName));
            client.indices()
                    .create(b -> b.index(indexName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/{indexName}")
    public void insertIndex(@PathVariable String indexName){
        Thread task = new Thread(){
            @Override
            public void run() {
                TsvReader.indexFile("title.basics.tsv", indexName);
            }
        };
        task.start();
    }
}
