package co.empathy.academy.imdb.controllers;

import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
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
import jakarta.json.JsonObjectBuilder;
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
    private static final String AVERAGE_RATING = "averageRating";
    private static final String TITLE_TYPE = "titleType";

    ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    @Operation(summary = "Does a query on the database")
    @Parameter(name = "q", description = "The whole query")
    @Parameter(name = "type", description = "Title type. It must match exactly and can be one or more, separated" +
            "by commas")
    @Parameter(name = "genre", description = "Genre of the film. It must match exactly and can be one or more, " +
            "separated by commas.")
    @Parameter(name = "agg", description = "The field you want to aggregate. It has to match exactly.")
    @Parameter(name = "gte", description = "A number for the average rating to be greater than or equal.")
    @Parameter(name = "lt", description = "A number for the average rating to be less than.")
    @Parameter(name = "directorId", description = "Id of the director whose films you want to search.")
    @Parameter(name = "from", description = "Specifies the number of hits that are going to be skipped.")
    @Parameter(name = "size", description = "Specifies the size of the hits going to be returned.")
    @ApiResponse(responseCode = "200", description = "Documents obtained", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "400", description = "Bad request", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Internal Server Error", content =
            { @Content(mediaType = "application/json")})
    @GetMapping("/search")
    public String search(
            @RequestParam(required = true, defaultValue = "") String q,
            @RequestParam(required = false) Optional<List<String>> type,
            @RequestParam(required = false) Optional<List<String>> genre,
            @RequestParam(required = false) Optional<String>agg,
            @RequestParam(required = false) Optional<String>gte,
            @RequestParam(required = false) Optional<String>lt,
            @RequestParam(required = false) Optional<String> directorId,
            @RequestParam(required = false) Optional<Integer> from,
            @RequestParam(required = false) Optional<Integer> size){
        SearchRequest.Builder request = new SearchRequest.Builder().index("films");
        BoolQuery.Builder boolQuery = new BoolQuery.Builder();
        from.ifPresent(request::from);
        size.ifPresent(request::size);
        addSearch(q, boolQuery);
        boostQueries(boolQuery, q);
        type.ifPresent(strings -> addFilter(strings, TITLE_TYPE, boolQuery));
        genre.ifPresent(strings -> addFilter(strings, "genres", boolQuery));
        agg.ifPresent(s -> addAgg(s, request));
        gte.ifPresent(s -> addGreaterThanOrEqualFilter(s, boolQuery));
        lt.ifPresent(s -> addLessThanFilter(s, boolQuery));
        directorId.ifPresent(s -> addDirectorFilter(s, boolQuery));
        request.query(sumScores(boolQuery.build()._toQuery())._toQuery());
        SearchResponse<JsonData> response = createResponse(request.build());

        if(response.hits().hits().isEmpty()){
            return SuggestionSearch.run(q);
        }else{
            return getResultsParsed(response, agg.orElse(null));
        }
    }

    @Operation(summary = "Retrieves the document with the id specified")
    @Parameter(name = "id", description = "Id of the document, it is mandatory")
    @ApiResponse(responseCode = "200", description = "Document obtained", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "400", description = "Bad request", content =
            { @Content(mediaType = "application/json")})
    @ApiResponse(responseCode = "500", description = "Internal Server Error", content =
            { @Content(mediaType = "application/json")})
    @GetMapping("/id_search")
    public String idSearch(
            @RequestParam(required = true) String id){
        SearchRequest.Builder request = new SearchRequest.Builder()
                .index("films");
        BoolQuery.Builder boolQuery = new BoolQuery.Builder();
        addSearchById(id, boolQuery);
        request.query(boolQuery.build()._toQuery());
        SearchResponse<JsonData> response = createResponse(request.build());
        return getResultsParsed(response, null);
    }

    private FunctionScoreQuery sumScores(Query boolQuery) {
        return QueryBuilders
                .functionScore()
                .query(boolQuery)
                .functions(FunctionScoreBuilders
                        .fieldValueFactor()
                        .field("numVotes")
                        .build()
                        ._toFunctionScore(),
                        FunctionScoreBuilders
                                .fieldValueFactor()
                                .field(AVERAGE_RATING)
                                .build()
                                ._toFunctionScore())
                .scoreMode(FunctionScoreMode.Multiply)
                .build();
    }

    private void boostQueries(BoolQuery.Builder boolQuery, String filmTitle) {
        boolQuery.should(
                QueryBuilders
                        .term()
                        .field("primaryTitle.raw")
                        .caseInsensitive(true)
                        .value(filmTitle)
                        .boost(10.0f)
                        .build().
                        _toQuery(),
                QueryBuilders
                        .term()
                        .field("originalTitle.raw")
                        .caseInsensitive(true)
                        .value(filmTitle)
                        .boost(9.0f)
                        .build().
                        _toQuery(),
                QueryBuilders
                        .term()
                        .field(TITLE_TYPE)
                        .value("movie")
                        .boost(9.0f)
                        .build().
                        _toQuery(),
                QueryBuilders
                        .term()
                        .field(TITLE_TYPE)
                        .value("tvMovie")
                        .boost(5.0f)
                        .build().
                        _toQuery()
        );
    }

    private void addAgg(String agg, SearchRequest.Builder request) {
        TermsAggregation termsAggregation = AggregationBuilders
                .terms()
                .field(agg)
                .size(1000)
                .build();
        request.aggregations(agg, termsAggregation._toAggregation());
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

    private void addDirectorFilter(String directorId, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        queries.add(
                QueryBuilders
                        .nested()
                        .path("directors")
                        .query(QueryBuilders
                                .match()
                                .field("directors.nconst")
                                .query(directorId)
                                .build()
                                ._toQuery())
                        .build()
                        ._toQuery()
        );
        boolQuery.filter(queries);
    }

    private void addGreaterThanOrEqualFilter(String string, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
            queries.add(QueryBuilders
                    .range()
                    .field(AVERAGE_RATING)
                    .gte(JsonData.of(string))
                    .build()._toQuery());
        boolQuery.filter(queries);
    }

    private void addLessThanFilter(String string, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        queries.add(QueryBuilders
                .range()
                .field(AVERAGE_RATING)
                .lt(JsonData.of(string))
                .build()._toQuery());
        boolQuery.filter(queries);
    }

    private void addSearch(String q, BoolQuery.Builder boolQuery) {
        if(q.isEmpty()){
            addMatchAllQuery(boolQuery);
        }else{
            addSearchWithQuery(q, boolQuery);
        }
    }

    private void addMatchAllQuery(BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        queries.add(QueryBuilders
                .matchAll()
                .build()
                ._toQuery());
        boolQuery.must(queries);
    }

    private void addSearchWithQuery(String q, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        queries.add(QueryBuilders
                .multiMatch()
                .fields("primaryTitle", "originalTitle",
                        "primaryTitle.english", "primaryTitle.raw")
                .query(q)
                .operator(Operator.And)
                .tieBreaker(0.5)
                .build()
                ._toQuery());
        boolQuery.must(queries);
    }

    private void addSearchById(String q, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        queries.add(QueryBuilders
                .match()
                .field("_id")
                .query(q)
                .build()
                ._toQuery());
        boolQuery.must(queries);
    }

    private JsonArrayBuilder parseHits(SearchResponse<JsonData> response) {
        JsonArrayBuilder hits = Json.createArrayBuilder();
        response.hits().hits().stream().filter(x -> x.source() != null).map(x ->
                Json.createObjectBuilder()
                        .add("id", x.id())
                        .add("source", x.source().toJson())
                        .build()
        ).toList().forEach(hits::add);
        return hits;
    }

    private JsonArrayBuilder parseAggregations(String aggName, SearchResponse<JsonData> response){
        List<StringTermsBucket> buckets = response
                .aggregations()
                .get(aggName)
                .sterms()
                .buckets()
                .array();

        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

        buckets.stream()
                .map(bucket -> Json.createObjectBuilder()
                        .add("key", bucket.key())
                        .add("doc_count", bucket.docCount())
                        .build())
                .toList().forEach(arrayBuilder::add);

        return arrayBuilder;
    }

    private String getResultsParsed(SearchResponse<JsonData> response, String agg) {
        JsonObjectBuilder result = Json.createObjectBuilder();
        result.add("hits", parseHits(response));

        if(agg != null)
            result.add("aggs", parseAggregations(agg, response));

        return result.build().toString();
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
