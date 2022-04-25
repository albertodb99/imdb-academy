package co.empathy.academy.imdb.exceptions;

public class IndexAlreadyExistsException extends RuntimeException {
    public IndexAlreadyExistsException(String message, Exception e) {
        super(message, e);
    }
}
