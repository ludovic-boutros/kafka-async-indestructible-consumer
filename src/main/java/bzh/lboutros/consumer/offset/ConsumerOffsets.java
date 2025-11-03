package bzh.lboutros.consumer.offset;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@AllArgsConstructor
@Builder
public class ConsumerOffsets {
    private final String clientId;
    private final Map<TopicPartition, PartitionOffsetAndMetadata> offsets = new ConcurrentHashMap<>();

    public void incrementOffsetPosition(ConsumerRecord<?, ?> record) {
        offsets.compute(new TopicPartition(record.topic(), record.partition()),
                (tp, pom) -> {
                    if (pom == null) {
                        return PartitionOffsetAndMetadata.builder()
                                .isRevoked(false)
                                .offsetAndMetadata(new OffsetAndMetadata(record.offset() + 1))
                                .build();
                    } else {
                        if (pom.isRevoked()) {
                            log.info("{} - Cannot increment offset to {} for topic-partition '{}-{}'. " +
                                            "Seems we have been revoked from this partition.",
                                    clientId,
                                    record.offset() + 1,
                                    record.topic(),
                                    record.partition());
                        } else {
                            pom.setOffsetAndMetadata(new OffsetAndMetadata(record.offset() + 1));
                        }

                        return pom;
                    }
                }
        );
    }

    public void revokePartition(TopicPartition topicPartition) {
        offsets.compute(topicPartition, (tp, pom) -> {
            if (pom == null) {
                log.warn("{} - Trying to revoke partition '{}' but no offset found for it.", clientId, topicPartition);
                return null;
            } else {
                pom.setRevoked(true);
                return pom;
            }
        });
    }

    public void addNewOffsetForPartition(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        offsets.put(topicPartition,
                PartitionOffsetAndMetadata.builder()
                        .isRevoked(false)
                        .offsetAndMetadata(offsetAndMetadata)
                        .build());
    }

    public PartitionOffsetAndMetadata getOffsetForPartition(TopicPartition topicPartition) {
        PartitionOffsetAndMetadata pom = offsets.get(topicPartition);
        // Return a copy of the internal object to avoid external modification.
        if (pom != null) {
            return PartitionOffsetAndMetadata.builder()
                    .isRevoked(pom.isRevoked())
                    .offsetAndMetadata(pom.getOffsetAndMetadata())
                    .build();
        } else {
            return null;
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @Builder
    public static class PartitionOffsetAndMetadata {
        private OffsetAndMetadata offsetAndMetadata;
        private boolean isRevoked;
    }
}
