package bzh.lboutros.consumer.offset;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

@Slf4j
public class InfiniteRetriesRebalanceListener implements ConsumerRebalanceListener {
    private final Consumer<?, ?> consumer;
    private final ConsumerOffsets offsets;

    public InfiniteRetriesRebalanceListener(Consumer<?, ?> consumer, ConsumerOffsets offsets) {
        this.consumer = consumer;
        this.offsets = offsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Revoked partitions: {}", partitions);
        for (TopicPartition topicPartition : partitions) {
            offsets.revokePartition(topicPartition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Assigned partitions: {}", partitions);
        Map<TopicPartition, OffsetAndMetadata> offsetsFromKafka = consumer.committed(new HashSet<>(partitions));
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetsEntry : offsetsFromKafka.entrySet()) {
            // Position can be unknown when you first initialize the group, let's be lazy here, when messages will be consumed, the map will be filled.
            if (offsetsEntry.getValue() != null) {
                offsets.addNewOffsetForPartition(offsetsEntry.getKey(), offsetsEntry.getValue());
            }
        }
    }
}