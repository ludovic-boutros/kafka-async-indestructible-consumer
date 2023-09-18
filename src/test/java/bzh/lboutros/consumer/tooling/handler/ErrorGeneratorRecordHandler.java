package bzh.lboutros.consumer.tooling.handler;

import bzh.lboutros.consumer.task.exception.RetriableException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class ErrorGeneratorRecordHandler implements Function<ConsumerRecord<?, ?>, Void> {
    private static final String BOOM = "RECORD_HANDLER BOOM !!!";
    private boolean generateRetriableError = false;
    private boolean generateNotRetriableError = false;
    private boolean stopTheWorld = false;

    private CompletableFuture<ConsumerRecord<?, ?>> futureRecord;

    public void generateRetriableError() {
        generateRetriableError = true;
    }

    public void generateNotRetriableError() {
        generateNotRetriableError = true;
    }

    private void stopTheWorld() {
        stopTheWorld = true;
    }

    public CompletableFuture<ConsumerRecord<?, ?>> resetFutureRecord() {
        futureRecord = new CompletableFuture<>();

        return futureRecord;
    }

    @Override
    public Void apply(ConsumerRecord<?, ?> record) {
        if (generateRetriableError) {
            log.info("Injecting RetriableException !");
            generateRetriableError = false;

            throw new RetriableException(BOOM);
        } else if (generateNotRetriableError) {
            log.info("Injecting IllegalStateException !");
            generateNotRetriableError = false;

            throw new IllegalStateException(BOOM);
        } else if (stopTheWorld) {
            log.info("Injecting Stop The World !");
            stopTheWorld = false;

            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        }

        log.info("{} -> {}", record.key(), record.value());
        futureRecord.complete(record);

        return null;
    }


}
