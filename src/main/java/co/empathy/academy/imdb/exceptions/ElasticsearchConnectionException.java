package co.empathy.academy.imdb.exceptions;

public class ElasticsearchConnectionException extends RuntimeException {
    public ElasticsearchConnectionException(String message, Exception e) {
        super(message, e);
    }
}
