package bzh.lboutros.consumer.task.exception;

public class RetriableException extends RuntimeException {
    public RetriableException(String message) {
        super(message);
    }
}
