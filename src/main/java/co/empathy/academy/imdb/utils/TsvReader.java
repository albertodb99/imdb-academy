package co.empathy.academy.imdb.utils;

import client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.springframework.util.ResourceUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

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


    /**
     * Method to index a file into an elasticsearch container.
     * @param fileName is the name of the file we want to index
     * @param indexName is the name of the index where we want to insert the file
     */
    public static void indexFile(String fileName, String indexName) {
        //First we have to read the file
        File file = readFile(fileName);
        //We insert the mapping
        insertMapping();
        //We initialize our control variables
        int counter = 0;
        int batch = 500;
        try (BufferedReader tsvReader = new BufferedReader(new FileReader(file))) {
            //We initialize our auxiliary variable
            String line = null;
            //We create the map
            Map<String, JsonObject> map = new HashMap<>();
            while ((line = tsvReader.readLine()) != null) {
                //We skip the first line (header)
                if (counter != 0) {
                    //Split it by tabs
                    String[] lineItems = line.split("\t");
                    //We create the jsonObject
                    JsonObject object = parseStringToJsonObject(lineItems);
                    //And we add it to the map
                    map.put(lineItems[HEADER], object);
                }
                counter++;
                //If we reach the desired number of iterations, we do the bulk request
                if(counter % batch == 0) {
                    doBulkRequest(map, indexName);
                    //We clear the map
                    map.clear();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method that reads a file from a string with the filename
     * @param fileName is the name of the file
     * @return the file converted
     */
    private static File readFile(String fileName) {
        File file = new File("");
        try {
            file = ResourceUtils.getFile("classpath:" + fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return file;
    }

    /**
     * Method that gets an array of strings and convert them to the JSON
     * structure that we need.
     * @param lineItems is the array of strings we want to parse to JSON
     * @return the JsonObject formatted.
     */
    private static JsonObject parseStringToJsonObject(String[] lineItems) {
        return Json.createObjectBuilder()
                .add("titleType", lineItems[TITLE_TYPE])
                .add("primaryTitle", lineItems[PRIMARY_TITLE])
                .add("originalTitle", lineItems[ORIGINAL_TITLE])
                .add("isAdult", parseBoolean(lineItems[IS_ADULT]))
                .add("startYear", lineItems[START_YEAR])
                .add("endYear", parseInteger(lineItems[END_YEAR]))
                .add("runtimeMinutes", parseInteger(lineItems[RUNTIME_MINUTES]))
                .add("genres", lineItems[GENRES])
                .build();
    }

    /**
     * Auxiliary method to help with the mapping. It tries to parse an
     * String to an Integer, if it is not possible, a 0 is returned
     * @param lineItem is the String we want to parse
     * @return the string parsed to an integer
     */
    private static int parseInteger(String lineItem) {
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
    private static void doBulkRequest(Map<String, JsonObject> map, String indexName) {
        try {
            client.bulk(first -> first
                    .operations(map.keySet().stream().map(x ->
                            BulkOperation.of(second -> second
                                    .index(third -> third
                                            .index(indexName)
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
    private static boolean parseBoolean(String lineItem) {
        return !lineItem.equals("0");
    }

    /**
     * Method that puts the map we want to the index of films.
     */
    private static void insertMapping() {
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
