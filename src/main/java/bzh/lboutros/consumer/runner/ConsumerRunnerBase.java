package bzh.lboutros.consumer.runner;

import bzh.lboutros.consumer.offset.ConsumerOffsets;
import bzh.lboutros.consumer.serializer.ErrorProofDeserializer;
import bzh.lboutros.consumer.task.InfiniteRetryRecordHandlingTask;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public abstract class ConsumerRunnerBase implements ConsumerRunner {
    @Getter
    private final String name;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutorService taskExecutor;
    private final Properties consumerProperties;
    private final String topicName;
    private final List<Pair<ConsumerRecords<?, ?>, CompletableFuture<Void>>> completableFutures;
    private final ConsumerOffsets offsets = new ConsumerOffsets();
    private boolean stopped;

    public ConsumerRunnerBase(String name, int threadCount, Properties consumerProperties, String topicName) {
        this.name = name;
        this.topicName = topicName;

        this.consumerProperties = new Properties();
        this.consumerProperties.putAll(consumerProperties);
        this.consumerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, name);
        this.consumerProperties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, name);
        completableFutures = new ArrayList<>();
        stopped = false;

        taskExecutor = Executors.newFixedThreadPool(threadCount, r -> new Thread(r, getName()));
    }

    @Override
    public void start() {
        executor.submit(() -> {
            while (!stopped) {
                try (KafkaConsumer<?, ?> consumer =
                             new KafkaConsumer<>(consumerProperties,
                                     new ErrorProofDeserializer<>(getKeyDeserializer()),
                                     new ErrorProofDeserializer<>(getValueDeserializer()))) {
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

    @Override
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
            // Something appended here, let's just warn for now, it should be reprocessed later
            log.warn("Error while committing offsets...", e);
        }
    }
}
