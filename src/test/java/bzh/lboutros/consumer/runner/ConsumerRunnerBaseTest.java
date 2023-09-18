package bzh.lboutros.consumer.runner;

import bzh.lboutros.consumer.KafkaTestUtils;
import bzh.lboutros.consumer.tooling.handler.ErrorGeneratorExceptionHandler;
import bzh.lboutros.consumer.tooling.handler.ErrorGeneratorRecordHandler;
import bzh.lboutros.consumer.tooling.runner.ErrorGeneratorConsumerRunner;
import bzh.lboutros.consumer.tooling.serializer.ErrorGeneratorStringDeserializer;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static bzh.lboutros.consumer.tooling.runner.ErrorGeneratorConsumerRunner.TOPIC_NAME;
import static bzh.lboutros.consumer.tooling.serializer.ErrorGeneratorStringDeserializer.ERROR_MESSAGE_VALUE;

class ConsumerRunnerBaseTest {
    private static KafkaContainer kafka;
    private final ErrorGeneratorRecordHandler recordHandler = new ErrorGeneratorRecordHandler();
    private final ErrorGeneratorExceptionHandler exceptionHandler = new ErrorGeneratorExceptionHandler();

    private final Properties properties;

    public ConsumerRunnerBaseTest() {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5_000);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfig.DEFAULT_MAX_POLL_RECORDS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorGeneratorStringDeserializer.class.getName());
    }

    @BeforeAll
    static void beforeAll() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
        kafka.start();
    }

    @AfterAll
    static void afterAll() {
        kafka.stop();
    }

    @Test
    @SneakyThrows
    void consumer_should_continue_to_process_despite_poison_pill() {
        // Given
        try (KafkaProducer<String, String> producer = KafkaTestUtils.getNewProducer(kafka.getBootstrapServers());
             ConsumerRunner runner = ErrorGeneratorConsumerRunner.builder()
                     .recordHandler(recordHandler)
                     .exceptionHandler(exceptionHandler)
                     .threadCount(1)
                     .consumerProperties(properties)
                     .retryInterval(1_000)
                     .build()) {
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
             ConsumerRunner runner = ErrorGeneratorConsumerRunner.builder()
                     .recordHandler(recordHandler)
                     .exceptionHandler(exceptionHandler)
                     .threadCount(1)
                     .consumerProperties(properties)
                     .retryInterval(1_000)
                     .build()) {
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
             ConsumerRunner runner = ErrorGeneratorConsumerRunner.builder()
                     .recordHandler(recordHandler)
                     .exceptionHandler(exceptionHandler)
                     .threadCount(1)
                     .consumerProperties(properties)
                     .retryInterval(1_000)
                     .build()) {
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
             ConsumerRunner runner = ErrorGeneratorConsumerRunner.builder()
                     .recordHandler(recordHandler)
                     .exceptionHandler(exceptionHandler)
                     .threadCount(1)
                     .consumerProperties(properties)
                     .retryInterval(1_000)
                     .build()) {
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
             ConsumerRunner runner = ErrorGeneratorConsumerRunner.builder()
                     .recordHandler(recordHandler)
                     .exceptionHandler(exceptionHandler)
                     .threadCount(1)
                     .consumerProperties(properties)
                     .retryInterval(1_000)
                     .build()) {
            CompletableFuture<List<ConsumerRecord<?, ?>>> futureRecords = recordHandler.resetFutureRecords(501);

            // When
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value_stop_the_world")).get();
            for (int i = 0; i < 500; i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "next_value_" + i)).get();
            }
            runner.start();

            List<ConsumerRecord<?, ?>> consumerRecords = futureRecords.get(30, TimeUnit.SECONDS);
            // Then
            Assertions.assertEquals(501, consumerRecords.size());
            Assertions.assertEquals("value_stop_the_world", consumerRecords.get(0).value());
        }
    }
}