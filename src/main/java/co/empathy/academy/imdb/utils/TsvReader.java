package co.empathy.academy.imdb.utils;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.indices.CloseIndexRequest;
import co.elastic.clients.elasticsearch.indices.OpenRequest;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.empathy.academy.imdb.exceptions.ElasticsearchConnectionException;
import co.empathy.academy.imdb.model.Film;
import co.empathy.academy.imdb.model.Rating;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TsvReader {
    static ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    private static final int BATCH_SIZE = 100000;
    private static final Logger logger = LoggerFactory.getLogger(TsvReader.class);
    private static final String FILMS = "films";

    public void indexFile(String filmsPath, String ratingsPath, String akasPath, String crewPath, String episodesPath, String principalPath, String nameBasicsPath) {
        try {
            logger.info("Adding mapping and indexing");
            insertMappingAndSettings();
            logger.info("Finished mapping and indexing");
            logger.info("Started indexing");
            var batchReader = new BatchReader(filmsPath, ratingsPath, akasPath, crewPath, episodesPath, principalPath, nameBasicsPath, BATCH_SIZE);

            while(!batchReader.hasFinished()) {
                List<JsonReference> batch = batchReader.getBatch();
                createBulkOperationsFromFilms(batch);
            }

            logger.info("Indexed");
            batchReader.close();
        } catch(IOException | ElasticsearchException e) {
            throw new ElasticsearchConnectionException("There was a problem processing your request", e);
        }
    }


    private void createBulkOperationsFromFilms(List<JsonReference> filmsParsed) throws IOException {
        List<BulkOperation> operations = new ArrayList<>();
        for (JsonReference film : filmsParsed) {
            operations.add(createBulkOperationFromFilm(film));
        }
        doBulkRequest(operations);
    }

    private BulkOperation createBulkOperationFromFilm(JsonReference film) {
        IndexOperation<Object> indexOperation = new IndexOperation.Builder<>()
                .index(FILMS)
                .document(film.json())
                .id(film.id())
                .build();
        return new BulkOperation.Builder()
                .index(indexOperation)
                .build();
    }


    /**
     * Method that receives a Map and do a BulkRequest of the operations contained
     * in that map.
     */
    private void doBulkRequest(List<BulkOperation> operations) throws IOException {
        client.bulk(first -> first
                .operations(operations)
        );
    }

    /**
     * Method that puts the map we want to the index of films.
     */
    private void insertMappingAndSettings() throws IOException {
        InputStream mapping = getClass().getClassLoader().getResourceAsStream("mapping.json");
        InputStream settings = getClass().getClassLoader().getResourceAsStream("settings.json");
        closeFilmsIndex();
        putSettingsToDatabase(settings);
        openFilmsIndex();
        putMappingToIndexFilms(mapping);
    }

    private void putMappingToIndexFilms(InputStream mapping) throws IOException {
        client.indices().putMapping(new PutMappingRequest
                .Builder()
                .index(FILMS)
                .withJson(mapping)
                .build());
    }

    private void putSettingsToDatabase(InputStream settings) throws IOException {
        client.indices().putSettings(new PutIndicesSettingsRequest.Builder()
                .withJson(settings)
                .build());
    }

    private void openFilmsIndex() throws IOException {
        client.indices().open(new OpenRequest
                .Builder()
                .index(FILMS)
                .build());
    }

    private void closeFilmsIndex() throws IOException {
        client.indices().close(new CloseIndexRequest
                .Builder()
                .index(FILMS)
                .build());
    }
}
