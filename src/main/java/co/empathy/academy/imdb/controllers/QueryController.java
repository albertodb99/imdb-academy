package co.empathy.academy.imdb.controllers;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api")
public class QueryController {

    ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    @GetMapping("/search")
    public String search(
            @RequestParam(required = false) Optional<String> q,
            @RequestParam(required = false) Optional<List<String>> type,
            @RequestParam(required = false) Optional<List<String>> genre,
            @RequestParam(required = false) Optional<String>agg){

        SearchRequest.Builder request = new SearchRequest.Builder().index("films");
        BoolQuery.Builder boolQuery = new BoolQuery.Builder();
        q.ifPresent(s -> addSearch(s, boolQuery));
        type.ifPresent(strings -> addFilter(strings, boolQuery));
        genre.ifPresent(strings -> addFilter(strings, boolQuery));
        agg.ifPresent(s -> addAgg(s, request));

        request.query(boolQuery.build()._toQuery());
        SearchResponse<JsonData> response = null;
        try {
            response = createResponse(request.build());
            return parseHits(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    private void addAgg(String agg, SearchRequest.Builder request) {
        TermsAggregation termsAggregation = AggregationBuilders
                .terms()
                .field(agg)
                .build();
        request.aggregations("agg_" + agg, termsAggregation._toAggregation());
    }

    private void addFilter(List<String> strings, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        for(String string : strings){
            queries.add(QueryBuilders
                    .term()
                    .field("genres")
                    .value(string)
                    .build()
                    ._toQuery());
        }
    }

    private void addSearch(String q, BoolQuery.Builder boolQuery) {
        List<Query> queries = new ArrayList<>();
        queries.add(QueryBuilders
                .match()
                .field("primaryTitle")
                .query(q)
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

    private SearchResponse<JsonData> createResponse(SearchRequest request) throws IOException {
        return client.search(request, JsonData.class);
    }


}
