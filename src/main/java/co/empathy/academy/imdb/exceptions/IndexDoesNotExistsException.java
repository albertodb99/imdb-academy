package co.empathy.academy.imdb.exceptions;

public class IndexDoesNotExistsException extends RuntimeException {
    public IndexDoesNotExistsException(String message, Exception e) {
        super(message, e);
    }
}
