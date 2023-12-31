package bzh.lboutros.consumer.task;

import bzh.lboutros.consumer.offset.ConsumerOffsets;
import bzh.lboutros.consumer.runner.ConsumerRunner;
import bzh.lboutros.consumer.task.exception.RetriableException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.decorators.Decorators;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.function.Function;

@Slf4j
public class InfiniteRetryRecordHandlingTask extends RecordHandlingTask {
    private final Retry infiniteRetry;

    @Builder
    public InfiniteRetryRecordHandlingTask(ConsumerRecords<?, ?> records,
                                           ConsumerRunner consumer,
                                           ConsumerOffsets offsets,
                                           int retryInterval) {
        super(records, consumer, offsets);

        infiniteRetry =
                Retry.of("infinite",
                        RetryConfig.custom()
                                .maxAttempts(Integer.MAX_VALUE)
                                .consumeResultBeforeRetryAttempt((i, result) ->
                                        log.error("A retriable error occurred, retrying {}:{}", i, result))
                                .intervalFunction(
                                        IntervalFunction.of(Duration.ofMillis(retryInterval)))
                                .writableStackTraceEnabled(true)
                                .retryExceptions(RetriableException.class)
                                .build());
    }

    @Override
    public void handle(ConsumerRecord<?, ?> record, Function<ConsumerRecord<?, ?>, Void> recordHandler) {
        // Process the record with a defined infinite retry strategy
        Decorators.ofFunction(recordHandler)
                .withRetry(infiniteRetry)
                .apply(record);
    }
}
