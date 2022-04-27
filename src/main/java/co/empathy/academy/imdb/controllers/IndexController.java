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
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Tag(name = "Index controller", description = "Allows to do several operations regarding indices.")
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

    @Operation(summary = "Creates an index in the database")
    @Parameter(name = "indexName", description = "The name of the index we want to create", required = true)
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

    @Operation(summary = "Deletes an index on the database")
    @Parameter(name = "indexName", description = "The name of the index we want to delete", required = true)
    @ApiResponse(responseCode = "200", description = "Index deleted", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "400", description = "The index does not exist", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Internal Server Error", content =
            { @Content(mediaType = "application/json")})
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

    @Operation(summary = "Insert documents in the index films")
    @Parameter(name = "filmsPath", description = "The path of the films .tsv that has to be indexed",
            required = true)
    @Parameter(name = "ratingsPathOpt", description = "The path of the ratings of the films .tsv that has to be indexed",
            required = false)
    @ApiResponse(responseCode = "200", description = "Documents indexed", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Internal Server Error", content =
            { @Content(mediaType = "application/json")})
    @PostMapping("/index_documents")
    public void insertIndex(@RequestParam String filmsPath, @RequestParam Optional<String> ratingsPathOpt ){
        ratingsPathOpt.ifPresentOrElse(
                ratingsPath -> new Thread(new TsvReader(filmsPath, ratingsPath)::indexFile).start(),
                () -> new Thread(new TsvReader(filmsPath)::indexFile).start()
        );
    }

}
