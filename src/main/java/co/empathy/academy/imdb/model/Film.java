package co.empathy.academy.imdb.model;

public class Film {

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

    private static final int HEADER = 0;
    private static final int TITLE_TYPE = 1;
    private static final int PRIMARY_TITLE = 2;
    private static final int ORIGINAL_TITLE = 3;
    private static final int IS_ADULT = 4;
    private static final int START_YEAR = 5;
    private static final int END_YEAR = 6;
    private static final int RUNTIME_MINUTES = 7;
    private static final int GENRES = 8;

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
        this.genres = parsed[GENRES].split(",");
        this.rating = null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitleType() {
        return titleType;
    }

    public void setTitleType(String titleType) {
        this.titleType = titleType;
    }

    public String getPrimaryTitle() {
        return primaryTitle;
    }

    public void setPrimaryTitle(String primaryTitle) {
        this.primaryTitle = primaryTitle;
    }

    public String getOriginalTitle() {
        return originalTitle;
    }

    public void setOriginalTitle(String originalTitle) {
        this.originalTitle = originalTitle;
    }

    public boolean isAdult() {
        return isAdult;
    }

    public void setAdult(boolean adult) {
        isAdult = adult;
    }

    public int getStartYear() {
        return startYear;
    }

    public void setStartYear(int startYear) {
        this.startYear = startYear;
    }

    public int getEndYear() {
        return endYear;
    }

    public void setEndYear(int endYear) {
        this.endYear = endYear;
    }

    public int getRuntimeMinutes() {
        return runtimeMinutes;
    }

    public void setRuntimeMinutes(int runtimeMinutes) {
        this.runtimeMinutes = runtimeMinutes;
    }

    public String[] getGenres() {
        return genres;
    }

    public void setGenres(String[] genres) {
        this.genres = genres;
    }

    public Rating getRating() {
        return rating;
    }

    public void setRating(Rating rating) {
        this.rating = rating;
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

    public Film getFilm(){
        return this;
    }
}
