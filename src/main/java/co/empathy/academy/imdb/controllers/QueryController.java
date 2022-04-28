package co.empathy.academy.imdb.controllers;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.empathy.academy.imdb.exceptions.ElasticsearchConnectionException;
import co.empathy.academy.imdb.exceptions.IndexNotFoundException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Tag(name = "Query controller", description = "Allows to do queries. You can do simple queries as well as queries " +
        "with filters and aggregations.")
@RestController
@RequestMapping("/api")
public class QueryController {

    ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    @Operation(summary = "Does a query on the database")
    @Parameter(name = "q", description = "The whole query")
    @Parameter(name = "type", description = "Title type. It must match exactly and can be one or more, separated" +
            "by commas")
    @Parameter(name = "genre", description = "Genre of the film. It must match exactly and can be one or more, " +
            "separated by commas.")
    @Parameter(name = "agg", description = "The field you want to aggregate. It has to match exactly.")
    @ApiResponse(responseCode = "200", description = "Documents obtained", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "400", description = "Bad request", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Internal Server Error", content =
            { @Content(mediaType = "application/json")})
    @GetMapping("/search")
    public String search(
            @RequestParam(required = false) Optional<String> q,
            @RequestParam(required = false) Optional<List<String>> type,
            @RequestParam(required = false) Optional<List<String>> genre,
            @RequestParam(required = false) Optional<String>agg){

        SearchRequest.Builder request = new SearchRequest.Builder().index("films");
        BoolQuery.Builder boolQuery = new BoolQuery.Builder();
        q.ifPresent(s -> addSearch(s, boolQuery));
        type.ifPresent(strings -> addFilter(strings, "titleType", boolQuery));
        genre.ifPresent(strings -> addFilter(strings, "genres", boolQuery));
        agg.ifPresent(s -> addAgg(s, request));

        request.query(boolQuery.build()._toQuery());
        SearchResponse<JsonData> response = createResponse(request.build());

        return agg.isPresent() ? parseAggregations("agg_" + agg.get(), response) : parseHits(response);
    }

    private void addAgg(String agg, SearchRequest.Builder request) {
        TermsAggregation termsAggregation = AggregationBuilders
                .terms()
                .field(agg)
                .build();
        request.aggregations("agg_" + agg, termsAggregation._toAggregation());
    }

    private void addFilter(List<String> strings, String field, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        for(String string : strings){
            queries.add(QueryBuilders
                    .term()
                    .field(field)
                    .value(string)
                    .build()
                    ._toQuery());
        }
        boolQuery.filter(queries);
    }

    private void addSearch(String q, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        queries.add(QueryBuilders
                .match()
                .field("primaryTitle")
                .query(q)
                        .boost(Float.valueOf("10.0"))
                .build()
                ._toQuery());
        boolQuery.must(queries);
    }

    private String parseHits(SearchResponse<JsonData> response) {
        return response.hits()
                .hits()
                .stream()
                .filter(hit -> hit.source() != null)
                .map(hit -> hit.source().toJson()).toList().toString();
    }

    private String parseAggregations(String aggName, SearchResponse<JsonData> response){
        List<StringTermsBucket> buckets = response
                .aggregations()
                .get(aggName)
                .sterms()
                .buckets()
                .array();

        Stream<JsonObject> jsonObjectStream = buckets.stream()
                .map(bucket -> Json.createObjectBuilder()
                        .add("key", bucket.key())
                        .add("doc_count", bucket.docCount())
                        .build());

        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

        jsonObjectStream.forEach(arrayBuilder::add);

        return arrayBuilder.build().toString();
    }

    private SearchResponse<JsonData> createResponse(SearchRequest request){
        try {
            return client.search(request, JsonData.class);
        } catch (IOException e) {
            throw new ElasticsearchConnectionException("Unable to connect to ElasticSearch", e);
        } catch(ElasticsearchException e){
           throw new IndexNotFoundException("Unable to process your request", e);
        }
    }


}
