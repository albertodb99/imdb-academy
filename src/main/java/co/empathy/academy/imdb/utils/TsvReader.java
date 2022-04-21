package co.empathy.academy.imdb.utils;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
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

    private static final String STANDARD = "standard";
    private static final int BATCH_SIZE = 25000;

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
        Map<String, Film> filmsMap;
        List<Rating> ratings;
        long start, end;
        try {
            //We insert the mapping
            insertMapping();
            start = System.currentTimeMillis();
            filmsMap = getFilmsMapFromFile();
            end = System.currentTimeMillis();
            System.out.println("Elapsed time in reading films file is: " + (end - start) + "ms");
            if(ratingsPath != null) {
                start = System.currentTimeMillis();
                ratings = getRatingsListFromFile();
                end = System.currentTimeMillis();
                System.out.println("Elapsed time in reading ratings file is: " + (end - start) + "ms");
                start = System.currentTimeMillis();
                mergeFilmsAndRatings(filmsMap, ratings);
                end = System.currentTimeMillis();
                System.out.println("Elapsed time in merging collections is: " + (end - start) + "ms");
            }
            List<JsonReference> filmsParsed = parseFilmsFromMap(filmsMap);
            doBulkOperations(filmsParsed);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void doBulkOperations(List<JsonReference> filmsParsed) throws IOException {
        List<BulkOperation> operations = new ArrayList<>();
        long start, end;
        for(JsonReference film : filmsParsed){
            IndexOperation<Object> indexOperation = new IndexOperation.Builder<>()
                    .index("films")
                    .document(film.getJson())
                    .id(film.getId())
                    .build();
            BulkOperation bulkOperation = new BulkOperation.Builder()
                    .index(indexOperation)
                    .build();
            operations.add(bulkOperation);
            if(operations.size() >= BATCH_SIZE) {
                start = System.currentTimeMillis();
                doBulkRequest(operations);
                end = System.currentTimeMillis();
                System.out.println("Elapsed time in doing 25.000 requests is: " + (end - start) + "ms");
                operations.clear();
            }
        }
    }

    private List<JsonReference> parseFilmsFromMap(Map<String, Film> filmsMap) {
        return new ArrayList<Film>(filmsMap.values())
                .stream()
                .map(this::parseStringToJson)
                .toList();
    }

    private void mergeFilmsAndRatings(Map<String, Film> filmsMap, List<Rating> ratings) {
        for(Rating r : ratings){
            Film f = filmsMap.get(r.getId());
            if(f != null)
                r.copyToFilm(f);
        }
    }

    private List<Rating> getRatingsListFromFile() throws IOException {
        return Files.readAllLines(Paths.get(ratingsPath))
                .stream()
                .skip(1)
                .map(Rating::new)
                .toList();
    }

    private Map<String, Film> getFilmsMapFromFile() throws IOException {
        return Files.readAllLines(Paths.get(filmsPath))
                .stream()
                .skip(1)
                .map(Film::new)
                .collect(Collectors.toMap(Film::getId, Film::getFilm));
    }

    /**
     * Method that gets an array of strings and convert them to the JSON
     * structure that we need.
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
        if(ratingsPath != null){
            if(film.getRating() == null)
                film.setRating(new Rating(film));
            json.add("averageRating", film.getRating().getAverageRating());
            json.add("numVotes", film.getRating().getAverageRating());
        }
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
    private void insertMapping() throws IOException {
            PutMappingRequest.Builder request = new PutMappingRequest.Builder()
                    .index("films")
                    .properties("titleType", second -> second
                            .text(third -> third
                                    .analyzer(STANDARD)
                                    .fields("raw", fourth -> fourth
                                            .keyword(fifth -> fifth)
                                    )
                            )
                    )
                    .properties("primaryTitle", second -> second
                            .text(third -> third
                                    .analyzer(STANDARD)
                                    .fields("raw", fourth -> fourth
                                            .keyword(fifth -> fifth)
                                    )
                            )
                    )
                    .properties("originalTitle", second -> second
                            .text(third -> third
                                    .analyzer(STANDARD)
                                    .fields("raw", fourth -> fourth
                                            .keyword(fifth -> fifth)
                                    )
                            )
                    )
                    .properties("isAdult", second -> second
                            .boolean_(third -> third)
                    )
                    .properties("startYear", second -> second
                            .integer(third -> third)
                    )
                    .properties("endYear", second -> second
                            .integer(third -> third.index(false)
                            )
                    )
                    .properties("runtimeMinutes", second -> second
                            .integer(third -> third.index(false)
                            )
                    )
                    .properties("genres", second -> second
                            .text(third -> third
                                    .analyzer(STANDARD)
                                    .fields("raw", fourth -> fourth
                                            .keyword(fifth -> fifth)
                                    )
                            )
                    );
            if(ratingsPath != null){
                request
                        .properties("averageRating", first -> first
                                .float_(second -> second))
                        .properties("numVotes", first -> first
                                .integer(second -> second));
            }
            client.indices().putMapping(request.build());
    }
}
