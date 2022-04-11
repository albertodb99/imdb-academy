package co.empathy.academy.imdb.utils;

import client.ClientCustomConfiguration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
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
