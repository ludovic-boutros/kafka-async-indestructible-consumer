package bzh.lboutros.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;

public class ConsumerOffsets extends HashMap<TopicPartition, OffsetAndMetadata> {

    public void incrementOffsetPosition(ConsumerRecord<?, ?> record) {
        put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
    }
}
