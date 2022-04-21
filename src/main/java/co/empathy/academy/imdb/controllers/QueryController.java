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
import java.util.Optional;

@RestController
@RequestMapping("/api")
public class QueryController {

    ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    @GetMapping("/search")
    public String simpleQuery(@RequestParam String q){
        try {
            SearchResponse<JsonData> response = createResponse(q);
            return parseHits(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private String parseHits(SearchResponse<JsonData> response) {
        return response.hits()
                .hits()
                .stream()
                .filter(hit -> hit.source() != null)
                .map(hit -> hit.source().toJson()).toList().toString();
    }

    private SearchResponse<JsonData> createResponse(String q) throws IOException {
        return client.search(first -> first
                .query(second -> second
                        .queryString(third -> third
                                .query(q)
                                .defaultField("primaryTitle")
                        )
                ) , JsonData.class
        );
    }


}
