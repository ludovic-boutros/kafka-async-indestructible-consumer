package bzh.lboutros.consumer.tooling.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ErrorGeneratorStringDeserializer extends StringDeserializer {
    public static final String ERROR_MESSAGE_VALUE = "value_should_be_in_error";
    private static final String BOOM = "SERIALIZER BOOM !!!";

    private void generateSerializationException(byte[] data) {
        if (new String(data).equals(ERROR_MESSAGE_VALUE)) {
            throw new SerializationException(BOOM);
        }
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        generateSerializationException(data);
        return super.deserialize(topic, data);
    }

    @Override
    public String deserialize(String topic, Headers headers, byte[] data) {
        generateSerializationException(data);
        return super.deserialize(topic, headers, data);
    }
}
