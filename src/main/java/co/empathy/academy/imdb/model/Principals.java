package co.empathy.academy.imdb.model;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Principals {
    private static final int NCONST = 2;
    private static final int CHARACTERS = 5;

    private Principals() {}

    public static void addPrincipals(List<String> principalsLines, JsonObjectBuilder builder, List<String> akasHeaders) {
        var principalsArray = Json.createArrayBuilder();
        for(String principalLine : principalsLines) {
            var fields = principalLine.split("\t");

            JsonArrayBuilder charactersJsonBuilder = Json.createArrayBuilder();
            toJsonArray(fields[CHARACTERS], charactersJsonBuilder);
            principalsArray.add(Json.createObjectBuilder()
                    .add("name", Name.addName(fields[NCONST]))
                    .add(akasHeaders.get(CHARACTERS), charactersJsonBuilder)
            );
        }

        builder.add("principals", principalsArray);
    }

    private static void toJsonArray(String string, JsonArrayBuilder builder) {
        if(!string.equals("\\N")) {
            String noBraces = string.substring(1, string.length() - 1);
            Arrays.stream(noBraces.split(",")).map(s -> s.substring(1, s.length() - 1)).forEach(builder::add);
        } else {
            builder.add(string);
        }
    }
}
