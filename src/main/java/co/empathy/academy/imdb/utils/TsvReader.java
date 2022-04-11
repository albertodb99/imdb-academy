package co.empathy.academy.imdb.utils;

import client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.BooleanProperty;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.util.ObjectBuilder;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.io.*;
import java.util.ArrayList;

public class TsvReader {
    static ElasticsearchClient client = new ClientCustomConfiguration().getElasticsearchCustomClient();

    public static void indexFile(String fileName, String indexName) {
        File file = new File("");
        try {
            file = ResourceUtils.getFile("classpath:" + fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //We try to read it
        //We insert the mapping
        insertMapping();
        int counter = 0;
        try (BufferedReader TSVReader = new BufferedReader(new FileReader(file))) {
            //We initialize the our auxiliary variable
            String line = null;
            //Until we reach the end of the file
            while ((line = TSVReader.readLine()) != null) {
                if(counter != 0) {
                    //Split it by tabs
                    String[] lineItems = line.split("\t");
                    JsonObject object = Json.createObjectBuilder()
                            .add("titleType", lineItems[1])
                            .add("primaryTitle", lineItems[2])
                            .add("originalTitle", lineItems[3])
                            .add("isAdult", lineItems[4])
                            .add("startYear", lineItems[5])
                            .add("endYear", lineItems[6])
                            .add("runtimeMinutes", lineItems[7])
                            .add("genres", lineItems[8])
                            .build();
                    insertIndex(object, lineItems[0], indexName);
                }
                counter++;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void insertMapping(){
        try {
            client.indices().putMapping(_0 -> _0
                    .index("films")
                    .properties("titleType", _1 -> _1
                            .text(_2 -> _2.analyzer("standard")))
                    .properties("primaryTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard")
                                    .fields("raw", _3 -> _3
                                            .keyword(_4 -> _4))))
                    .properties("originalTitle", _1 -> _1
                            .text(_2 -> _2
                                    .analyzer("standard")
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
                                    analyzer("standard"))));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static boolean insertIndex(JsonObject object, String id, String indexName){
        boolean created = false;
        Reader jsonReader = new StringReader(object.toString());
        try{
              client.index(builder -> builder
                    .index(indexName)
                    .id(id)
                    .withJson(jsonReader));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return created;
    }
}
