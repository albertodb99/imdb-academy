package co.empathy.academy.imdb.controllers;

import co.empathy.academy.imdb.client.ElasticHighCustomConfiguration;
import co.empathy.academy.imdb.exceptions.ElasticsearchConnectionException;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.IOException;
public class SuggestionSearch {
        private static RestHighLevelClient client = ElasticHighCustomConfiguration.getClient();
        private SuggestionSearch() {}

        public static String run(String q) {
            var searchRequest = new SearchRequest("films");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SuggestBuilder suggestBuilder = new SuggestBuilder();
            addTermSuggestion(q, suggestBuilder);
            addPhraseSuggestion(q, suggestBuilder);
            searchSourceBuilder.suggest(suggestBuilder);
            searchRequest.source(searchSourceBuilder);

            try {
                var suggestResponse = client.search(searchRequest, RequestOptions.DEFAULT);

                TermSuggestion suggest = suggestResponse.getSuggest().getSuggestion("spellcheck");
                PhraseSuggestion phraseSuggest = suggestResponse.getSuggest().getSuggestion("phrase-suggester");

                var optionArray = Json.createArrayBuilder();
                var phraseArray = Json.createArrayBuilder();

                addTermResults(suggest, optionArray);
                addPhraseResults(phraseSuggest, phraseArray);

                return Json.createObjectBuilder()
                        .add("hits", Json.createArrayBuilder().build())
                        .add("aggs", Json.createArrayBuilder().build())
                        .add("term-suggestions", optionArray.build())
                        .add("phrase-suggestions", phraseArray.build())
                        .build()
                        .toString();

            } catch (IOException e) {
                throw new ElasticsearchConnectionException("There was a problem processing your request", e);
            }
        }

    private static void addPhraseResults(PhraseSuggestion phraseSuggest, JsonArrayBuilder phraseArray) {
        phraseSuggest.getEntries().get(0).getOptions()
                .stream().map(option ->
                        Json.createObjectBuilder()
                                .add("score", option.getScore())
                                .add("text", option.getText().toString())
                                .build())
                .forEach(phraseArray::add);
    }

    private static void addTermResults(TermSuggestion suggest, JsonArrayBuilder optionArray) {
        suggest.getEntries().get(0).getOptions()
                .stream().map(option ->
                        Json.createObjectBuilder()
                                .add("score", option.getScore())
                                .add("freq", option.getFreq())
                                .add("text", option.getText().toString())
                                .build())
                .forEach(optionArray::add);
    }


    private static void addTermSuggestion(String q, SuggestBuilder suggestBuilder) {
        SuggestionBuilder<TermSuggestionBuilder> termSuggestionBuilder =
                SuggestBuilders.termSuggestion("primaryTitle").text(q);
        suggestBuilder.addSuggestion("spellcheck", termSuggestionBuilder);
    }

    private static void addPhraseSuggestion(String q, SuggestBuilder builder) {
        SuggestionBuilder<PhraseSuggestionBuilder> phraseSuggestionBuilder =
                SuggestBuilders.phraseSuggestion("primaryTitle")
                        .text(q)
                        .gramSize(3)
                        .maxErrors(3)
                        .analyzer("custom_films_analyzer");
        builder.addSuggestion("phrase-suggester", phraseSuggestionBuilder);
    }
}
