package co.empathy.academy.imdb.model;

import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class Film {
    private static final int HEADER = 0;
    private static final int TITLE_TYPE = 1;
    private static final int PRIMARY_TITLE = 2;
    private static final int ORIGINAL_TITLE = 3;
    private static final int IS_ADULT = 4;
    private static final int START_YEAR = 5;
    private static final int END_YEAR = 6;
    private static final int RUNTIME_MINUTES = 7;
    private static final int GENRES_LIST = 8;

    private String id;
    private String titleType;
    private String primaryTitle;
    private String originalTitle;
    private boolean isAdult;
    private int startYear;
    private int endYear;
    private int runtimeMinutes;
    private String[] genres;
    private Rating rating;

    private Film(){}

    public Film(String toParse){
        String[] parsed = toParse.split("\t");
        this.id = parsed[HEADER];
        this.titleType = parsed[TITLE_TYPE];
        this.primaryTitle = parsed[PRIMARY_TITLE];
        this.originalTitle = parsed[ORIGINAL_TITLE];
        this.isAdult = parseStringToBoolean(parsed[IS_ADULT]);
        this.startYear = parseStringToInt(parsed[START_YEAR]);
        this.endYear = parseStringToInt(parsed[END_YEAR]);
        this.runtimeMinutes = parseStringToInt(parsed[RUNTIME_MINUTES]);
        this.genres = parsed[GENRES_LIST].split(",");
    }

    public static void addFilm(String line, JsonObjectBuilder builder, List<String> headers) {
        String[] fields = line.split("\t");

        var arrayBuilder = Json.createArrayBuilder();

        Arrays.stream(fields[GENRES_LIST].split(",")).forEach(arrayBuilder::add);

        builder.add(headers.get(HEADER), fields[HEADER])
                .add(headers.get(TITLE_TYPE), fields[TITLE_TYPE])
                .add(headers.get(PRIMARY_TITLE), fields[PRIMARY_TITLE])
                .add(headers.get(ORIGINAL_TITLE), fields[ORIGINAL_TITLE])
                .add(headers.get(IS_ADULT), parseStringToBoolean(fields[IS_ADULT]))
                .add(headers.get(START_YEAR), parseStringToInt(fields[START_YEAR]))
                .add(headers.get(END_YEAR), parseStringToInt(fields[END_YEAR]))
                .add(headers.get(RUNTIME_MINUTES), parseStringToInt(fields[RUNTIME_MINUTES]))
                .add(headers.get(GENRES_LIST), arrayBuilder.build());
    }


    /**
     * Method that parses a String to boolean
     * @param line is the string we want to parse
     * @return the boolean converted
     */
    private static boolean parseStringToBoolean(String line) {
        return !line.equals("0");
    }

    /**
     * Auxiliary method to help with the mapping. It tries to parse an
     * String to an Integer, if it is not possible, a 0 is returned
     * @param line is the String we want to parse
     * @return the string parsed to an integer
     */
    private static int parseStringToInt(String line) {
        int toRet;
        try {
            toRet = Integer.parseInt(line);
        }catch(NumberFormatException e){
            toRet = 0;
        }
        return toRet;
    }

    public Film getIdentity(){
        return this;
    }

    public int getIdToCompare() {
        return Integer.parseInt(this.id.split("tt")[1]);
    }
}
