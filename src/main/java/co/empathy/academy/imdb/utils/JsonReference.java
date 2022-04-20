package co.empathy.academy.imdb.utils;

import jakarta.json.JsonObject;

public class JsonReference {
    private String id;
    private JsonObject json;

    public JsonReference(String id, JsonObject json) {
        setId(id);
        setJson(json);
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public JsonObject getJson() {
        return this.json;
    }

    public void setJson(JsonObject json) {
        this.json = json;
    }
}
