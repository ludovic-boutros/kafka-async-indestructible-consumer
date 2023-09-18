package bzh.lboutros.consumer.tooling.handler;

import bzh.lboutros.consumer.task.exception.RetriableException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class ErrorGeneratorRecordHandler implements Function<ConsumerRecord<?, ?>, Void> {
    private static final String BOOM = "RECORD_HANDLER BOOM !!!";
    private final List<ConsumerRecord<?, ?>> consumedRecords = new ArrayList<>();
    private int expectedRecordCount;
    private boolean generateRetriableError = false;
    private boolean generateNotRetriableError = false;
    private boolean stopTheWorld = false;
    private CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords;

    public void generateRetriableError() {
        generateRetriableError = true;
    }

    public void generateNotRetriableError() {
        generateNotRetriableError = true;
    }

    public void stopTheWorld() {
        stopTheWorld = true;
    }

    public CompletableFuture<List<ConsumerRecord<?, ?>>> resetFutureRecords(int expectedRecordCount) {
        this.expectedRecordCount = expectedRecordCount;
        consumedRecords.clear();
        futureRecords = new CompletableFuture<>();

        return futureRecords;
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

        Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
        log.info("{} -> {}", record.key(), record.value());
        consumedRecords.add(record);
        if (consumedRecords.size() == expectedRecordCount) {
            futureRecords.complete(consumedRecords);
        }

        return null;
    }


}
