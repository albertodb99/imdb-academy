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
        try (BufferedReader TSVReader = new BufferedReader(new FileReader(file))) {
            //We initialize our auxiliary variable
            String line = null;
            //We create the map
            Map<String, JsonObject> map = new HashMap<>();
            while ((line = TSVReader.readLine()) != null) {
                //We skip the first line (header)
                if (counter != 0) {
                    //Split it by tabs
                    String[] lineItems = line.split("\t");
                    //We create the jsonObject
                    JsonObject object = parseStringToJsonObject(lineItems);
                    //And we add it to the map
                    map.put(lineItems[0], object);
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
                .add("titleType", lineItems[1])
                .add("primaryTitle", lineItems[2])
                .add("originalTitle", lineItems[3])
                .add("isAdult", parseBoolean(lineItems[4]))
                .add("startYear", lineItems[5])
                .add("endYear", parseInteger(lineItems[6]))
                .add("runtimeMinutes", parseInteger(lineItems[7]))
                .add("genres", lineItems[8])
                .build();
    }

    /**
     * Auxiliary method to help with the mapping. It tries to parse an
     * String to an Integer, if it is not possible, a 0 is returned
     * @param lineItem is the String we want to parse
     * @return the string parsed to an integer
     */
    private static int parseInteger(String lineItem) {
        int toRet = 0;
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
            client.bulk(_0 -> _0
                    .operations(map.keySet().stream().map(x ->
                            BulkOperation.of(_1 -> _1
                                    .index(_2 -> _2
                                            .index(indexName)
                                            .id(x)
                                            .document(map.get(x))))).toList()
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
            client.indices().putMapping(_0 -> _0
                    .index("films")
                    .properties("titleType", _1 -> _1
                            .text(_2 -> _2.analyzer(STANDARD)))
                    .properties("primaryTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer(STANDARD)
                                    .fields("raw", _3 -> _3
                                            .keyword(_4 -> _4))))
                    .properties("originalTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer(STANDARD)
                                    .fields("raw", _3 -> _3
                                            .keyword(_4 -> _4))))
                    .properties("isAdult", _1 -> _1
                            .boolean_(_2 -> _2))
                    .properties("startYear", _1 -> _1
                            .integer(_2 -> _2))
                    .properties("endYear", _1 -> _1
                            .integer(_2 -> _2.index(false)))
                    .properties("runtimeMinutes", _1 -> _1
                            .integer(_2 -> _2.index(false)))
                    .properties("genres", _1 -> _1
                            .text(_2 -> _2.
                                    analyzer(STANDARD))));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
