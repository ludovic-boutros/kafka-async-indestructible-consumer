package bzh.lboutros.consumer.runner;

import bzh.lboutros.consumer.offset.ConsumerOffsets;
import bzh.lboutros.consumer.offset.InfiniteRetriesRebalanceListener;
import bzh.lboutros.consumer.serializer.ErrorProofDeserializer;
import bzh.lboutros.consumer.task.InfiniteRetryRecordHandlingTask;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static bzh.lboutros.consumer.runner.SafeConsumerFunctions.*;
import static bzh.lboutros.consumer.serializer.ErrorProofDeserializer.DELEGATE_SUFFIX;

@Slf4j
public class ConsumerRunner implements Closeable {
    @Getter
    private final String name;
    @Getter
    private final Integer instanceId;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutorService taskExecutor;
    private final Properties consumerProperties;
    private final String topicName;
    private final List<Pair<ConsumerRecords<?, ?>, CompletableFuture<Void>>> completableFutures;
    private final ConsumerOffsets offsets;
    private final int retryInterval;
    @Getter
    private final Function<ConsumerRecord<?, ?>, Void> recordHandler;
    @Getter
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> exceptionHandler;
    private boolean stopped;

    @Builder
    public ConsumerRunner(@NonNull String name,
                          Integer instanceId,
                          int threadCount,
                          @NonNull Properties consumerProperties,
                          @NonNull String topicName,
                          int retryInterval,
                          @NonNull Function<ConsumerRecord<?, ?>, Void> recordHandler,
                          @NonNull BiConsumer<ConsumerRecord<?, ?>, Exception> exceptionHandler) {
        this.name = name;
        this.topicName = topicName;
        this.retryInterval = retryInterval;
        this.recordHandler = recordHandler;
        this.exceptionHandler = exceptionHandler;
        this.instanceId = instanceId != null ? instanceId : 0;
        this.offsets = new ConsumerOffsets(getClientId());

        this.consumerProperties = new Properties();
        this.consumerProperties.putAll(consumerProperties);
        this.consumerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, getClientId());
        this.consumerProperties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, name);
        // Use error proof deserializers and save configured ones
        this.consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + DELEGATE_SUFFIX,
                this.consumerProperties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        this.consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + DELEGATE_SUFFIX,
                this.consumerProperties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

        this.consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorProofDeserializer.class.getName());
        this.consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorProofDeserializer.class.getName());

        completableFutures = new ArrayList<>();
        stopped = false;

        taskExecutor = Executors.newFixedThreadPool(threadCount, r -> new Thread(r, getName()));
    }

    private String getClientId() {
        return this.name + "-" + this.instanceId;
    }

    public void start() {
        executor.submit(() -> {
            while (!stopped) {
                try (KafkaConsumer<?, ?> consumer =
                             new KafkaConsumer<>(consumerProperties)) {
                    consumer.subscribe(List.of(topicName), new InfiniteRetriesRebalanceListener(consumer, offsets));

                    while (!stopped) {
                        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(1000));

                        log.debug("Runner {}: Fetched {} records. Consumer is paused for partitions: {}", getClientId(), records.count(), consumer.paused());

                        if (!records.isEmpty()) {
                            if (safePause(consumer, records.partitions())) {
                                // If pause was successful, process records asynchronously.
                                // Otherwise, it means we these partitions are not assigned to us anymore.
                                // We need to poll again, and they will be reprocessed later.
                                asyncProcess(records);
                            }
                        }
                        checkForCompletedTasks(consumer);
                    }
                } catch (Exception ex) {
                    log.error("Runner {}: Fatal error during consumer poll, trying to reconnect...", getClientId(), ex);
                }
            }

            log.info("Runner {}: Terminating...", getClientId());
        });
    }

    public void stop() {
        stopped = true;
        taskExecutor.close();
        executor.close();
    }

    public void close() {
        stop();
    }

    private void asyncProcess(ConsumerRecords<?, ?> records) {
        completableFutures.add(
                Pair.of(
                        records,
                        CompletableFuture.runAsync(
                                InfiniteRetryRecordHandlingTask.builder()
                                        .consumer(this)
                                        .records(records)
                                        .offsets(offsets)
                                        .retryInterval(retryInterval)
                                        .build(),
                                taskExecutor
                        )
                )
        );
    }

    private void checkForCompletedTasks(KafkaConsumer<?, ?> consumer) {
        List<Pair<ConsumerRecords<?, ?>, CompletableFuture<Void>>> completedTasks = completableFutures.stream()
                .filter(t -> t.getRight().isDone())
                .toList();

        completedTasks.forEach(task -> {
            completableFutures.remove(task);

            safeCommit(consumer, task.getLeft().partitions(), offsets);
            safeResume(consumer, task.getLeft().partitions(), offsets);
        });
    }


}
