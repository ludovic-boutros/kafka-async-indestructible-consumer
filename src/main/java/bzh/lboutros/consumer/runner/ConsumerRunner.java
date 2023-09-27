package bzh.lboutros.consumer.runner;

import bzh.lboutros.consumer.offset.ConsumerOffsets;
import bzh.lboutros.consumer.serializer.ErrorProofDeserializer;
import bzh.lboutros.consumer.task.InfiniteRetryRecordHandlingTask;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static bzh.lboutros.consumer.serializer.ErrorProofDeserializer.DELEGATE_SUFFIX;

@Slf4j
public class ConsumerRunner implements Closeable {
    @Getter
    private final String name;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutorService taskExecutor;
    private final Properties consumerProperties;
    private final String topicName;
    private final List<Pair<ConsumerRecords<?, ?>, CompletableFuture<Void>>> completableFutures;
    private final ConsumerOffsets offsets = new ConsumerOffsets();
    private final int retryInterval;
    @Getter
    private final Function<ConsumerRecord<?, ?>, Void> recordHandler;
    @Getter
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> exceptionHandler;
    private boolean stopped;

    @Builder
    public ConsumerRunner(@NonNull String name,
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

        this.consumerProperties = new Properties();
        this.consumerProperties.putAll(consumerProperties);
        this.consumerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, name);
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

    public void start() {
        executor.submit(() -> {
            while (!stopped) {
                try (KafkaConsumer<?, ?> consumer =
                             new KafkaConsumer<>(consumerProperties)) {
                    consumer.subscribe(List.of(topicName));

                    while (!stopped) {
                        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(1000));

                        log.debug("Runner {}: Fetched {} records. Consumer is paused for partitions: {}", name, records.count(), consumer.paused());

                        if (!records.isEmpty()) {
                            consumer.pause(records.partitions());
                            asyncProcess(records);
                        }
                        checkForCompletedTasks(consumer);
                    }
                } catch (Exception ex) {
                    log.error("Runner {}: Fatal error during consumer poll, trying to reconnect...", name, ex);
                }
            }

            log.info("Runner {}: Terminating...", name);
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

            safeCommit(consumer, task.getLeft().partitions());
            consumer.resume(task.getLeft().partitions());
        });
    }

    private void safeCommit(KafkaConsumer<?, ?> consumer, Set<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> toBeCommitted = partitions.stream()
                .collect(Collectors.toMap(Function.identity(), offsets::get));

        try {
            consumer.commitSync(toBeCommitted);
        } catch (Exception e) {
            // Something happened here, let's just warn for now, it should be reprocessed later
            log.warn("Error while committing offsets...", e);
        }
    }
}
