package bzh.lboutros.consumer.tooling.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

public class ErrorGeneratorStringDeserializer extends StringDeserializer {
    public static final String ERROR_MESSAGE_VALUE = "value_should_be_in_error";
    private static final String BOOM = "SERIALIZER BOOM !!!";

    private boolean configured;

    public ErrorGeneratorStringDeserializer() {
        super();
        configured = false;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        configured = true;
    }

    private void generateSerializationException(byte[] data) {
        if (new String(data).equals(ERROR_MESSAGE_VALUE)) {
            throw new SerializationException(BOOM);
        }
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        assertConfigured();
        
        generateSerializationException(data);
        return super.deserialize(topic, data);
    }

    @Override
    public String deserialize(String topic, Headers headers, byte[] data) {
        assertConfigured();

        generateSerializationException(data);
        return super.deserialize(topic, headers, data);
    }

    private void assertConfigured() {
        if (!configured) {
            throw new IllegalStateException("Serializer not initialized first!");
        }
    }

}
