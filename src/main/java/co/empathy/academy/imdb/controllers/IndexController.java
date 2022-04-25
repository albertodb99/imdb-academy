package co.empathy.academy.imdb.controllers;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.imdb.exceptions.ElasticsearchConnectionException;
import co.empathy.academy.imdb.exceptions.IndexAlreadyExistsException;
import co.empathy.academy.imdb.exceptions.IndexDoesNotExistsException;
import co.empathy.academy.imdb.utils.TsvReader;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RestController
public class IndexController {

    ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    @Operation(summary = "Returns a list of the current indices in the database")
    @ApiResponse(responseCode = "200", description = "Indices obtained", content =
            { @Content(mediaType = "application/json")})
    @GetMapping("/_cat/indices")
    public Map<String, IndexState> getIndexes(){
        try {
            return client.indices().get( c -> c.index("*")).result();
        } catch (IOException    e) {
            e.printStackTrace();
        }
        return new HashMap<>();
    }

    @Operation(summary = "Does a query on the database")
    @Parameter(name = "indexName", description = "The name of the index we want to create")
    @ApiResponse(responseCode = "200", description = "Index created", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "400", description = "The index already exists", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Internal Server Error", content =
            { @Content(mediaType = "application/json")})
    @PutMapping("{indexName}")
    public boolean createIndex(@PathVariable String indexName){
        try {
            return Boolean.TRUE.equals(client.indices()
                    .create(b -> b.index(indexName)).acknowledged());
        } catch (IOException e) {
            throw new ElasticsearchConnectionException("Unable to connect to ElasticSearch", e);
        }
        catch (ElasticsearchException e) {
            throw new IndexAlreadyExistsException("The following index: " + indexName + " already exists", e);
        }
    }

    @DeleteMapping("{indexName}")
    public boolean deleteIndex(@PathVariable String indexName){
        try {
            return client.indices()
                    .delete(b -> b.index(indexName)).acknowledged();
        } catch (IOException e) {
            throw new ElasticsearchConnectionException("Unable to connect to ElasticSearch", e);
        }
        catch (ElasticsearchException e) {
            throw new IndexDoesNotExistsException("The following index: " + indexName + " does not exists", e);
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
