package co.empathy.academy.imdb.model;

import jakarta.json.JsonObjectBuilder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class Rating implements Indexable {

    private String id;
    @Getter @Setter
    private double averageRating;
    @Getter @Setter
    private int numVotes;

    private static final int HEADER = 0;
    private static final int AVERAGE_RATING = 1;
    private static final int NUM_VOTES = 2;

    private Rating(){}

    public Rating(String rating) {
        String[] parsed = rating.split("\t");
        this.id = parsed[HEADER];
        this.averageRating = parseStringToDouble(parsed[AVERAGE_RATING]);
        this.numVotes = parseStringToInt(parsed[NUM_VOTES]);
    }

    public Rating(Film film) {
        this.id = film.getId();
        this.averageRating = 0;
        this.numVotes = 0;
    }

    public static void addRating(String line, JsonObjectBuilder builder, List<String> headers) {
        String[] fields = line.split("\t");

        builder.add(headers.get(AVERAGE_RATING), parseStringToDouble(fields[AVERAGE_RATING]))
                .add(headers.get(NUM_VOTES), parseStringToInt(fields[NUM_VOTES]));
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void copyToFilm(Film f) {
        f.setRating(this);
    }

    private static int parseStringToInt(String line) {
        int toRet;
        try {
            toRet = Integer.parseInt(line);
        }catch(NumberFormatException e){
            toRet = 0;
        }
        return toRet;
    }

    private static double parseStringToDouble(String line) {
        double toRet;
        try {
            toRet = Double.parseDouble(line);
        }catch(NumberFormatException e){
            toRet = 0;
        }
        return toRet;
    }

    public Rating getIdentity() {
        return this;
    }
}
