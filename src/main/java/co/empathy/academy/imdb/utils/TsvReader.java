package co.empathy.academy.imdb.utils;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.indices.CloseIndexRequest;
import co.elastic.clients.elasticsearch.indices.OpenRequest;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.empathy.academy.imdb.model.Film;
import co.empathy.academy.imdb.model.Rating;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TsvReader {
    static ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    private static final int BATCH_SIZE = 100000;
    private static final String FILMS = "films";

    private String filmsPath;
    private String ratingsPath;

    public TsvReader(String filmsPath, String ratingsPath){
        this.filmsPath = filmsPath;
        this.ratingsPath = ratingsPath;
    }

    public TsvReader(String filmsPath){
        this.filmsPath = filmsPath;
    }

    /**
     * Method to index a file into an elasticsearch container.
     */
    public void indexFile() {
        //Now we read all the lines of the films file.
        List<String> films;
        try {
            insertMappingAndSettings();
            films = getFilmsFromFile();
            addFilmsToDatabaseInRanges(films);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void addFilmsToDatabaseInRanges(List<String> films) throws IOException {
        Map<String, Rating> ratings;
        int startingRange = 0;
        int endRange = BATCH_SIZE;
        ratings = getRatingsMapFromFile(ratingsPath);
        do {
            if (films.size() - endRange < BATCH_SIZE) {
                endRange = films.size() - 1;
            }
            List<Film> filmsList = getFilmsListFromSubset(films.subList(startingRange, endRange));
            mergeFilmsAndRatings(filmsList, ratings);
            List<JsonReference> filmsParsed = parseFilmsFromList(filmsList);
            createBulkOperationsFromFilms(filmsParsed);
            if (endRange != films.size() - 1) {
                startingRange += BATCH_SIZE;
                endRange += BATCH_SIZE;
            }
        } while (endRange < films.size() - 1);
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
                .document(film.getJson())
                .id(film.getId())
                .build();
        return new BulkOperation.Builder()
                .index(indexOperation)
                .build();
    }

    private List<JsonReference> parseFilmsFromList(List<Film> filmsList) {
        return filmsList
                .stream()
                .map(this::parseStringToJson)
                .toList();
    }

    private void mergeFilmsAndRatings(List<Film> filmsList, Map<String, Rating> ratingsMap) {
        for (Film f : filmsList) {
            Rating r = ratingsMap.get(f.getId());
            if (r != null) {
                r.copyToFilm(f);
            }
        }
    }

    private Map<String, Rating> getRatingsMapFromFile(String ratingsPath) throws IOException {
        return Files.readAllLines(Paths.get(ratingsPath).toAbsolutePath())
                .stream()
                .skip(1)
                .map(Rating::new)
                .collect(Collectors.toMap(Rating::getId, Rating::getIdentity));
    }

    private List<String> getFilmsFromFile() throws IOException {
        return Files.readAllLines(Paths.get(filmsPath).toAbsolutePath());
    }

    private List<Film> getFilmsListFromSubset(List<String> filmsSubSet) {
        return filmsSubSet
                .stream()
                .skip(1)
                .map(Film::new)
                .toList();
    }

    /**
     * Method that gets an array of strings and convert them to the JSON
     * structure that we need.
     *
     * @param film is the array of strings we want to parse to JSON
     * @return the JsonObject formatted.
     */
    private JsonReference parseStringToJson(Film film) {
        JsonArrayBuilder array = Json.createArrayBuilder();
        Arrays.stream(film.getGenres()).forEach(array::add);
        JsonObjectBuilder json = Json.createObjectBuilder()
                .add("titleType", film.getTitleType())
                .add("primaryTitle", film.getPrimaryTitle())
                .add("originalTitle", film.getOriginalTitle())
                .add("isAdult", film.isAdult())
                .add("startYear", film.getStartYear())
                .add("endYear", film.getEndYear())
                .add("runtimeMinutes", film.getRuntimeMinutes())
                .add("genres", array.build());
        if (film.getRating() == null)
            film.setRating(new Rating(film));
        json.add("averageRating", film.getRating().getAverageRating());
        json.add("numVotes", film.getRating().getNumVotes());
        return new JsonReference(film.getId(), json.build());
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
