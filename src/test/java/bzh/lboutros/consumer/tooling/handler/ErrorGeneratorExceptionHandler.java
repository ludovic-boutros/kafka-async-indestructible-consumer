package bzh.lboutros.consumer.tooling.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.BiConsumer;

@Slf4j
public class ErrorGeneratorExceptionHandler implements BiConsumer<ConsumerRecord<?, ?>, Exception> {
    private static final String BOOM = "EXCEPTION_HANDLER BOOM !!!";

    private boolean generateException = false;

    public void generateException() {
        generateException = true;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception ex) {
        log.error("Error with during record processing of: {} -> {} : {}",
                record.key(),
                record.value(),
                ExceptionUtils.getStackTrace(ex));

        if (generateException) {
            generateException = false;
            log.info("Generating IllegalStateException in the exception handler !");
            throw new IllegalStateException(BOOM);
        }
    }
}
