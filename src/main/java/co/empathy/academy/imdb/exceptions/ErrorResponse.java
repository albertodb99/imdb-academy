package co.empathy.academy.imdb.exceptions;

import co.elastic.clients.elasticsearch.nodes.Http;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;

@Getter
@Setter
public class ErrorResponse {

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss")
    private LocalDateTime timestamp;

    private HttpStatus status;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private String message;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private String stackTrace;

    private ErrorResponse(){
        this.timestamp = LocalDateTime.now();
    }

    ErrorResponse(HttpStatus status){
        this();
        this.status = status;
    }

    ErrorResponse(HttpStatus status, String stackTrace){
        this(status);
        this.message = "Unexpected error";
        this.stackTrace = stackTrace;
    }

    ErrorResponse(HttpStatus status, String message, String stackTrace){
        this(status, stackTrace);
        this.message = message;
    }

}
