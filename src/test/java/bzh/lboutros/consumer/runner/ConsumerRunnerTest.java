package bzh.lboutros.consumer.runner;

import bzh.lboutros.consumer.KafkaTestUtils;
import bzh.lboutros.consumer.tooling.handler.ErrorGeneratorExceptionHandler;
import bzh.lboutros.consumer.tooling.handler.ErrorGeneratorRecordHandler;
import bzh.lboutros.consumer.tooling.serializer.ErrorGeneratorStringDeserializer;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static bzh.lboutros.consumer.tooling.serializer.ErrorGeneratorStringDeserializer.ERROR_MESSAGE_VALUE;

class ConsumerRunnerTest {
    private static final String TOPIC_NAME = "my-topic";
    private static KafkaContainer kafka;
    private final ErrorGeneratorRecordHandler recordHandler = new ErrorGeneratorRecordHandler("client-0");
    private final ErrorGeneratorExceptionHandler exceptionHandler = new ErrorGeneratorExceptionHandler();

    private Properties properties;

    public ConsumerRunnerTest() {

    }

    @BeforeAll
    static void beforeAll() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
        kafka.start();

        try (var adminClient = KafkaTestUtils.getNewAdminClient(kafka.getBootstrapServers())) {
            adminClient.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic(TOPIC_NAME, 4, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic " + TOPIC_NAME, e);
        }
    }

    @AfterAll
    static void afterAll() {
        kafka.stop();
    }

    @BeforeEach
    void beforeEach() {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5_000);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfig.DEFAULT_MAX_POLL_RECORDS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorGeneratorStringDeserializer.class.getName());
    }

    @Test
    @SneakyThrows
    void consumer_should_continue_to_process_despite_poison_pill() {
        // Given
        try (KafkaProducer<String, String> producer = KafkaTestUtils.getNewProducer(kafka.getBootstrapServers());
             ConsumerRunner runner = getConsumerRunner()) {
            CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords = recordHandler.resetFutureRecords(1);

            // When
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", ERROR_MESSAGE_VALUE)).get();
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value_no_error")).get();
            runner.start();

            List<ConsumerRecord<?, ?>> consumerRecords = futureRecords.get(30, TimeUnit.SECONDS);
            // Then
            Assertions.assertEquals(1, consumerRecords.size());
            Assertions.assertEquals("value_no_error", consumerRecords.get(0).value());
        }
    }

    @Test
    @SneakyThrows
    void consumer_should_continue_to_process_despite_retriable_error() {
        // Given
        recordHandler.generateRetriableError();

        try (KafkaProducer<String, String> producer = KafkaTestUtils.getNewProducer(kafka.getBootstrapServers());
             ConsumerRunner runner = getConsumerRunner()) {
            CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords = recordHandler.resetFutureRecords(1);

            // When
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value_no_error")).get();
            runner.start();

            List<ConsumerRecord<?, ?>> consumerRecords = futureRecords.get(30, TimeUnit.SECONDS);
            // Then
            Assertions.assertEquals(1, consumerRecords.size());
            Assertions.assertEquals("value_no_error", consumerRecords.get(0).value());
        }
    }

    @Test
    @SneakyThrows
    void consumer_should_continue_to_process_despite_not_retriable_error() {
        // Given
        recordHandler.generateNotRetriableError();

        try (KafkaProducer<String, String> producer = KafkaTestUtils.getNewProducer(kafka.getBootstrapServers());
             ConsumerRunner runner = getConsumerRunner()) {
            CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords = recordHandler.resetFutureRecords(1);

            // When
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value_error_not_retriable")).get();
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value_no_error")).get();
            runner.start();

            List<ConsumerRecord<?, ?>> consumerRecords = futureRecords.get(30, TimeUnit.SECONDS);
            // Then
            Assertions.assertEquals(1, consumerRecords.size());
            Assertions.assertEquals("value_no_error", consumerRecords.get(0).value());
        }
    }

    @Test
    @SneakyThrows
    void consumer_should_continue_to_process_despite_exception_in_exception_handler() {
        // Given
        recordHandler.generateNotRetriableError();
        exceptionHandler.generateException();

        try (KafkaProducer<String, String> producer = KafkaTestUtils.getNewProducer(kafka.getBootstrapServers());
             ConsumerRunner runner = getConsumerRunner()) {
            CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords = recordHandler.resetFutureRecords(1);

            // When
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value_error_not_retriable")).get();
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value_no_error")).get();
            runner.start();

            List<ConsumerRecord<?, ?>> consumerRecords = futureRecords.get(30, TimeUnit.SECONDS);
            // Then
            Assertions.assertEquals(1, consumerRecords.size());
            Assertions.assertEquals("value_no_error", consumerRecords.get(0).value());
        }
    }

    @Test
    @SneakyThrows
    void consumer_should_continue_to_process_despite_stop_the_world() {
        // Given
        recordHandler.stopTheWorld();

        try (KafkaProducer<String, String> producer = KafkaTestUtils.getNewProducer(kafka.getBootstrapServers());
             ConsumerRunner runner = getConsumerRunner()) {
            CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords = recordHandler.resetFutureRecords(2);

            // When
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value_stop_the_world")).get();
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "next_value")).get();
            runner.start();

            List<ConsumerRecord<?, ?>> consumerRecords = futureRecords.get(30, TimeUnit.SECONDS);
            // Then
            Assertions.assertEquals(2, consumerRecords.size());
            Assertions.assertEquals("value_stop_the_world", consumerRecords.get(0).value());
            Assertions.assertEquals("next_value", consumerRecords.get(1).value());
        }
    }

    @Test
    @SneakyThrows
    void consumer_should_continue_to_process_despite_rebalance() {
        ErrorGeneratorRecordHandler recordHandler1 = new ErrorGeneratorRecordHandler("client-1");
        ErrorGeneratorRecordHandler recordHandler2 = new ErrorGeneratorRecordHandler("client-2");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

        ConsumerRunner.ConsumerRunnerBuilder runnerBuilder1 = ConsumerRunner.builder()
                .name("test")
                .instanceId(1)
                .topicName(TOPIC_NAME)
                .recordHandler(recordHandler1)
                .exceptionHandler(exceptionHandler)
                .threadCount(1)
                .consumerProperties(properties)
                .retryInterval(1_000);

        ConsumerRunner runner2 = null;

        try (KafkaProducer<String, String> producer = KafkaTestUtils.getNewProducer(kafka.getBootstrapServers());
             ConsumerRunner runner1 = runnerBuilder1.build()) {
            CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords1 = recordHandler1.resetFutureRecords(200);

            // When
            for (int i = 0; i < 200; i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, "key" + i, "value_" + i)).get();
            }

            runner1.start();

            List<ConsumerRecord<?, ?>> consumerRecords1 = futureRecords1.get(30, TimeUnit.SECONDS);

            futureRecords1 = recordHandler1.resetFutureRecords(400);
            CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords2 = recordHandler2.resetFutureRecords(400);
            for (int i = 200; i < 600; i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, "key" + i, "value_" + i)).get();
                if (i % 100 == 0) {
                    // Trigger a rebalance every 50 records
                    // Offset should never be committed by runner2, it should not have enough time to process a batch of records before being stopped
                    if (i / 100 % 2 == 0) {
                        runner2 = ConsumerRunner.builder()
                                .name("test")
                                .instanceId(2)
                                .topicName(TOPIC_NAME)
                                .recordHandler(recordHandler2)
                                .exceptionHandler(exceptionHandler)
                                .threadCount(1)
                                .consumerProperties(properties)
                                .retryInterval(1_000)
                                .build();
                        runner2.start();
                        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
                    } else {
                        runner2.stop();
                        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
                    }
                }
            }

            try {
                consumerRecords1 = futureRecords1.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                consumerRecords1 = recordHandler1.getConsumedRecords();
            }

            List<ConsumerRecord<?, ?>> consumerRecords2 = List.of();
            try {
                consumerRecords2 = futureRecords2.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                try {
                    consumerRecords2 = recordHandler2.getConsumedRecords();
                } catch (Exception e2) {
                    // Ignore
                }
            }

            // Then
            Assertions.assertTrue(400 <= consumerRecords1.size() + consumerRecords2.size());
        } finally {
            if (runner2 != null) {
                runner2.stop();
            }
        }
    }


    private ConsumerRunner getConsumerRunner() {
        return ConsumerRunner.builder()
                .name("test")
                .topicName(TOPIC_NAME)
                .recordHandler(recordHandler)
                .exceptionHandler(exceptionHandler)
                .threadCount(1)
                .consumerProperties(properties)
                .retryInterval(1_000)
                .build();
    }
}