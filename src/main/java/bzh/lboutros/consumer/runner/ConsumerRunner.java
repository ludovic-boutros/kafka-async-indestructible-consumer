package bzh.lboutros.consumer.runner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Closeable;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface ConsumerRunner extends Closeable {
    void start();

    void stop();

    Function<ConsumerRecord<?, ?>, Void> getRecordHandler();

    BiConsumer<ConsumerRecord<?, ?>, Exception> getExceptionHandler();

    Deserializer<?> getKeyDeserializer();

    Deserializer<?> getValueDeserializer();
}
