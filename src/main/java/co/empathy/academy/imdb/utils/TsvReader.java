package co.empathy.academy.imdb.utils;

import co.empathy.academy.imdb.client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import org.springframework.util.ResourceUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        List<String> filmsLines;
        List<String> ratingsLines;
        try {
            filmsLines = Files.readAllLines(Paths.get(filmsPath));
            if(ratingsPath != null)
                ratingsLines = Files.readAllLines(Paths.get(ratingsPath));
            for(int i = 1; i < filmsLines.size() ; i++){
                String[] filmsLineSplitted = filmsLines.get(i).split("\t");

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Method that gets an array of strings and convert them to the JSON
     * structure that we need.
     * @param lineItems is the array of strings we want to parse to JSON
     * @return the JsonObject formatted.
     */
    private JsonReference parseStringToJson(String[] lineItems) {
        JsonObjectBuilder json = Json.createObjectBuilder()
                .add("titleType", lineItems[TITLE_TYPE])
                .add("primaryTitle", lineItems[PRIMARY_TITLE])
                .add("originalTitle", lineItems[ORIGINAL_TITLE])
                .add("isAdult", parseBoolean(lineItems[IS_ADULT]))
                .add("startYear", lineItems[START_YEAR])
                .add("endYear", parseInteger(lineItems[END_YEAR]))
                .add("runtimeMinutes", parseInteger(lineItems[RUNTIME_MINUTES]))
                .add("genres", lineItems[GENRES]);
        if(ratingsPath != null){
            json.add("averageRating", AVERAGE_RATING);
            json.add("numVotes", NUM_VOTES);
        }
        return new JsonReference(lineItems[HEADER], json.build());
    }

    /**
     * Auxiliary method to help with the mapping. It tries to parse an
     * String to an Integer, if it is not possible, a 0 is returned
     * @param lineItem is the String we want to parse
     * @return the string parsed to an integer
     */
    private int parseInteger(String lineItem) {
        int toRet;
        try {
            toRet = Integer.parseInt(lineItem);
        }catch(NumberFormatException e){
            toRet = 0;
        }
        return toRet;
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
     * Method that parses a String to boolean
     * @param lineItem is the string we want to parse
     * @return the boolean converted
     */
    private boolean parseBoolean(String lineItem) {
        return !lineItem.equals("0");
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
