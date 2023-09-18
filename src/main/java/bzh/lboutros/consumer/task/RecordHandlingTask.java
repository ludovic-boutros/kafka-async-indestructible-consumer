package bzh.lboutros.consumer.task;

import bzh.lboutros.consumer.offset.ConsumerOffsets;
import bzh.lboutros.consumer.runner.ConsumerRunnerBase;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static bzh.lboutros.consumer.serializer.ErrorProofDeserializer.DESERIALIZATION_ERROR_HEADER;

@Slf4j
@AllArgsConstructor
public abstract class RecordHandlingTask implements Runnable {
    private final ConsumerRecords<?, ?> records;
    private final ConsumerRunnerBase consumer;
    private final ConsumerOffsets offsets;

    public abstract void handle(ConsumerRecord<?, ?> record, Function<ConsumerRecord<?, ?>, Void> recordHandler);

    @Override
    public void run() {
        for (ConsumerRecord<?, ?> record : records) {
            try {
                Header deserializationErrorHeader = record.headers().lastHeader(DESERIALIZATION_ERROR_HEADER);
                if (deserializationErrorHeader != null) {
                    // We got a serialization exception
                    consumer.getExceptionHandler()
                            .accept(record,
                                    new SerializationException(
                                            new String(deserializationErrorHeader.value(), StandardCharsets.UTF_8)));
                    continue;
                }
                // Here we process each record.
                // Consumer is paused for the current processed partitions.
                handle(record, consumer.getRecordHandler());
            } catch (Exception ex) {
                try {
                    // We got a processing exception
                    consumer.getExceptionHandler().accept(record, ex);
                } catch (Exception recordExceptionHandlerException) {
                    // Only can log something here
                    log.error("Exception during exception handler processing. " +
                                    "First exception was: {}. " +
                                    "Exception in the exception handler was: {}",
                            ExceptionUtils.getStackTrace(ex),
                            ExceptionUtils.getStackTrace(recordExceptionHandlerException));
                }
            }

            offsets.incrementOffsetPosition(record);
        }
    }
}
