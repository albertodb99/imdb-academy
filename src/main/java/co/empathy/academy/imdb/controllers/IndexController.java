package co.empathy.academy.imdb.controllers;

import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.imdb.utils.TsvReader;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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

    @GetMapping("/index_documents")
    public void insertIndex(@RequestParam String filmsPath, @RequestParam Optional<String> ratingsPathOpt ){
        ratingsPathOpt.ifPresentOrElse(
                ratingsPath -> new Thread(new TsvReader(filmsPath, ratingsPath)::indexFile).start(),
                () -> new Thread(new TsvReader(filmsPath)::indexFile).start()
        );
    }

}
