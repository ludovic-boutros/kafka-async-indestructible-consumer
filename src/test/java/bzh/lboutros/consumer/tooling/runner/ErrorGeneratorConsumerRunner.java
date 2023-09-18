package bzh.lboutros.consumer.tooling.runner;

import bzh.lboutros.consumer.runner.ConsumerRunnerBase;
import bzh.lboutros.consumer.tooling.serializer.ErrorGeneratorStringDeserializer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Slf4j
public class ErrorGeneratorConsumerRunner extends ConsumerRunnerBase {
    public static String TOPIC_NAME = "my-topic";
    private final StringDeserializer keyDeserializer;
    private final StringDeserializer valueDeserializer;
    private final Function<ConsumerRecord<?, ?>, Void> recordHandler;
    private final BiConsumer<ConsumerRecord<?, ?>, Exception> exceptionHandler;

    @Builder
    public ErrorGeneratorConsumerRunner(Properties consumerProperties,
                                        int threadCount,
                                        Function<ConsumerRecord<?, ?>, Void> recordHandler,
                                        BiConsumer<ConsumerRecord<?, ?>, Exception> exceptionHandler) {
        super("error-consumer", threadCount, consumerProperties, "my-topic");
        this.recordHandler = recordHandler;
        this.exceptionHandler = exceptionHandler;

        keyDeserializer = new StringDeserializer();
        valueDeserializer = new ErrorGeneratorStringDeserializer();
    }

    @Override
    public Function<ConsumerRecord<?, ?>, Void> getRecordHandler() {
        return recordHandler;
    }

    @Override
    public BiConsumer<ConsumerRecord<?, ?>, Exception> getExceptionHandler() {
        return exceptionHandler;
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public Deserializer<?> getKeyDeserializer() {
        return keyDeserializer;
    }

    @Override
    public Deserializer<?> getValueDeserializer() {
        return valueDeserializer;
    }
}
