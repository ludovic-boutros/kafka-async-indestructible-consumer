package bzh.lboutros.consumer.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class ErrorProofDeserializer<T> implements Deserializer<T> {

    public static final String DESERIALIZATION_ERROR_HEADER = "DESERIALIZATION_ERROR";
    private final Deserializer<T> delegate;

    public ErrorProofDeserializer(Deserializer<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return delegate.deserialize(topic, data);
        } catch (Exception ex) {
            log.error("Error during deserialization of message: {}", ExceptionUtils.getStackTrace(ex));
        }
        return null;
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
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
}
