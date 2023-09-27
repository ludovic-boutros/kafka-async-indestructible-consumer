package bzh.lboutros.consumer.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class ErrorProofDeserializer implements Deserializer<Object> {
    public static final String DELEGATE_SUFFIX = ".delegate";

    public static final String DESERIALIZATION_ERROR_HEADER = "DESERIALIZATION_ERROR";
    private Deserializer<?> delegate;

    public ErrorProofDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String className = (String) configs.get(isKey ?
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + DELEGATE_SUFFIX :
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + DELEGATE_SUFFIX
        );

        try {
            delegate = createDeserializerInstance(className);
            delegate.configure(configs, isKey);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            return delegate.deserialize(topic, data);
        } catch (Exception ex) {
            log.error("Error during deserialization of message: {}", ExceptionUtils.getStackTrace(ex));
        }
        return null;
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        try {
            return delegate.deserialize(topic, headers, data);
        } catch (Exception ex) {
            String stackTrace = ExceptionUtils.getStackTrace(ex);
            log.error("Error during deserialization of message: {}", stackTrace);
            headers.add(DESERIALIZATION_ERROR_HEADER, stackTrace.getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

    @Override
    public void close() {
        delegate.close();
    }

    private Deserializer<?> createDeserializerInstance(String className) throws ReflectiveOperationException {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor = clazz.getConstructor();
        Object object = constructor.newInstance();
        if (!(object instanceof Deserializer)) {
            throw new IllegalArgumentException(className + " is not a deserializer.");
        }

        return (Deserializer<?>) object;
    }
}
