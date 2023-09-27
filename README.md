# Async processing Kafka Consumer Pattern

This project is a simple example of how to implement external system calls like long SQL queries or HTTP API
using async processing and pause/resume pattern on consumers.

[Resilience4j](https://github.com/resilience4j/resilience4j) is used for easy to use retry strategy implementation.

# Usage

First you need to create two `handlers`:

- a record handler implementing `Function<ConsumerRecord<?, ?>, Void>`;
- an exception handler implementing `BiConsumer<ConsumerRecord<?, ?>, Exception>`.

Put your business logic in the record handler.

You need to map your retriable exceptions to [RetriableException](src/main/java/bzh/lboutros/consumer/task/exception/RetriableException.java).

In this simple example, the retry strategy is a [simple infinite retry strategy](src/main/java/bzh/lboutros/consumer/task/InfiniteRetryRecordHandlingTask.java) in case of `RetriableException`.

The exception handler is only used in case of non retriable exception.

Then, you just need to build and start a `ConsumerRunner` instance:

```java
ConsumerRunner runner = ConsumerRunner.builder()
        .recordHandler(recordHandler)
        .exceptionHandler(exceptionHandler)
        .threadCount(1)
        .consumerProperties(properties)
        .retryInterval(1_000)
        .build()
        .start();
```

Deserializers are decorated using [an error proof](src/main/java/bzh/lboutros/consumer/serializer/ErrorProofDeserializer.java) implementation.

# Testing

Testcontainer is used for testing.
On macos, you may need to use these two properties to make it work with Colima:

```properties
DOCKER_HOST=unix:///Users/<your_user_account>/.colima/default/docker.sock
TESTCONTAINERS_RYUK_DISABLED=true
```