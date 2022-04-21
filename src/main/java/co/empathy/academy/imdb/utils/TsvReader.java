package co.empathy.academy.imdb.utils;

import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.empathy.academy.imdb.model.Film;
import co.empathy.academy.imdb.model.Rating;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import org.springframework.util.ResourceUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TsvReader {
    static ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    private static final String STANDARD = "standard";
    private static final int HEADER = 0;
    private static final int TITLE_TYPE = 1;
    private static final int PRIMARY_TITLE = 2;
    private static final int ORIGINAL_TITLE = 3;
    private static final int IS_ADULT = 4;
    private static final int START_YEAR = 5;
    private static final int END_YEAR = 6;
    private static final int RUNTIME_MINUTES = 7;
    private static final int GENRES = 8;
    private static final int AVERAGE_RATING = 9;
    private static final int NUM_VOTES = 10;

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
        //We insert the mapping
        insertMapping();
        //Now we read all the lines of the films file.
        Map<String, Film> filmsMap;
        List<Rating> ratings;
        try {
            filmsMap = Files.readAllLines(Paths.get(filmsPath)).stream().map(Film::new).collect(
                    Collectors.toMap(Film::getId, Film::getFilm));
            if(ratingsPath != null) {
                ratings = Files.readAllLines(Paths.get(ratingsPath))
                        .stream()
                        .map(rating -> new Rating(rating))
                        .toList();
                for(Rating r : ratings){
                    Film f = filmsMap.get(r.getId());
                    if(f != null)
                        r.copyToFilm(f);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Method that gets an array of strings and convert them to the JSON
     * structure that we need.
     * @param film is the array of strings we want to parse to JSON
     * @return the JsonObject formatted.
     */
    private JsonReference parseStringToJson(Film film) {
        JsonObjectBuilder json = Json.createObjectBuilder()
                .add("titleType", film.getTitleType())
                .add("primaryTitle", film.getPrimaryTitle())
                .add("originalTitle", film.getOriginalTitle())
                .add("isAdult", film.isAdult())
                .add("startYear", film.getStartYear())
                .add("endYear", film.getEndYear())
                .add("runtimeMinutes", film.getRuntimeMinutes())
                .add("genres", Arrays.toString(film.getGenres()));
        if(ratingsPath != null){
            json.add("averageRating", AVERAGE_RATING);
            json.add("numVotes", NUM_VOTES);
        }
        return new JsonReference(film.getId(), json.build());
    }

    /**
     * Method that receives a Map and do a BulkRequest of the operations contained
     * in that map.
     * @param map is the map that contains the operations we want to do.
     */
    private void doBulkRequest(Map<String, JsonObject> map) {
        try {
            client.bulk(first -> first
                    .operations(map.keySet().stream().map(x ->
                            BulkOperation.of(second -> second
                                    .index(third -> third
                                            .index("films")
                                            .id(x)
                                            .document(map.get(x)
                                            )
                                    )
                            )
                            ).toList()
                    )
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method that puts the map we want to the index of films.
     */
    private void insertMapping() {
        try {
            client.indices().putMapping(first -> first
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
                    )
            );

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
