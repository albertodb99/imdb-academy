package co.empathy.academy.imdb.controllers;

import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import jakarta.json.JsonValue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api")
public class QueryController {

    ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    @GetMapping("/q")
    public List<JsonValue> get(@RequestParam String query){
        try {
            SearchResponse<JsonData> response = client.search(first -> first
                    .query(second -> second
                            .queryString(third -> third
                                    .query(query)
                                    .defaultField("primaryTitle")
                            )
                    ) , JsonData.class
            );
            return response.hits()
                    .hits()
                    .stream()
                    .filter(hit -> hit.source() != null)
                    .map(hit -> hit.source().toJson()).toList();
        } catch (IOException e) {

        }
        return null;
    }


}
