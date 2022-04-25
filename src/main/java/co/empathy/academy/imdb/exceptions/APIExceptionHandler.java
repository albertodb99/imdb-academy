package co.empathy.academy.imdb.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class APIExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(value = {ElasticsearchConnectionException.class})
    public ResponseEntity<ErrorResponse> handleConnectionError(RuntimeException e) {
        return createResponseEntity(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()
        , e.getCause().toString()));
    }

    @ExceptionHandler(value = {IndexAlreadyExistsException.class, IndexDoesNotExistsException.class,
            IndexNotFoundException.class})
    public ResponseEntity<ErrorResponse> handleBadRequest(RuntimeException e) {
        return createResponseEntity(new ErrorResponse(HttpStatus.BAD_REQUEST, e.getMessage()
                , e.getCause().toString()));
    }

    private ResponseEntity<ErrorResponse> createResponseEntity(ErrorResponse errorResponse) {
        return new ResponseEntity<>(errorResponse, errorResponse.getStatus());
    }
}
