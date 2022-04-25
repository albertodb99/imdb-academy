package co.empathy.academy.imdb.exceptions;

public class IndexNotFoundException extends RuntimeException {
    public IndexNotFoundException(String message, Exception e) {
        super(message, e);
    }
}
