package co.empathy.academy.imdb.model;

import jakarta.json.Json;
import jakarta.json.JsonObject;

public class Name {
    private Name(){}

    public static JsonObject addName(String id) {
        return Json.createObjectBuilder()
                .add("nconst", id)
                .build();
    }
}
