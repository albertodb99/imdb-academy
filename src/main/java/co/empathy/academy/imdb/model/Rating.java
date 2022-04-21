package co.empathy.academy.imdb.model;

public class Rating implements Indexable {

    private String id;
    private double averageRating;
    private int numVotes;

    private static final int HEADER = 0;
    private static final int AVERAGE_RATING = 1;
    private static final int NUM_VOTES = 2;


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

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(double averageRating) {
        this.averageRating = averageRating;
    }

    public int getNumVotes() {
        return numVotes;
    }

    public void setNumVotes(int numVotes) {
        this.numVotes = numVotes;
    }

    @Override
    public void copyToFilm(Film f) {
        f.setRating(this);
    }

    private int parseStringToInt(String line) {
        int toRet;
        try {
            toRet = Integer.parseInt(line);
        }catch(NumberFormatException e){
            toRet = 0;
        }
        return toRet;
    }

    private double parseStringToDouble(String line) {
        double toRet;
        try {
            toRet = Double.parseDouble(line);
        }catch(NumberFormatException e){
            toRet = 0;
        }
        return toRet;
    }
}
