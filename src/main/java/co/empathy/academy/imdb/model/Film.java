package co.empathy.academy.imdb.model;

import lombok.Getter;
import lombok.Setter;
public class Film {

    @Getter @Setter
    private String id;
    @Getter @Setter
    private String titleType;
    @Getter @Setter
    private String primaryTitle;
    @Getter @Setter
    private String originalTitle;
    @Getter @Setter
    private boolean isAdult;
    @Getter @Setter
    private int startYear;
    @Getter @Setter
    private int endYear;
    @Getter @Setter
    private int runtimeMinutes;
    @Getter @Setter
    private String[] genres;
    @Getter @Setter
    private Rating rating;

    private static final int HEADER = 0;
    private static final int TITLE_TYPE = 1;
    private static final int PRIMARY_TITLE = 2;
    private static final int ORIGINAL_TITLE = 3;
    private static final int IS_ADULT = 4;
    private static final int START_YEAR = 5;
    private static final int END_YEAR = 6;
    private static final int RUNTIME_MINUTES = 7;
    private static final int GENRES_LIST = 8;

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


    /**
     * Method that parses a String to boolean
     * @param line is the string we want to parse
     * @return the boolean converted
     */
    private boolean parseStringToBoolean(String line) {
        return !line.equals("0");
    }

    /**
     * Auxiliary method to help with the mapping. It tries to parse an
     * String to an Integer, if it is not possible, a 0 is returned
     * @param line is the String we want to parse
     * @return the string parsed to an integer
     */
    private int parseStringToInt(String line) {
        int toRet;
        try {
            toRet = Integer.parseInt(line);
        }catch(NumberFormatException e){
            toRet = 0;
        }
        return toRet;
    }
}
