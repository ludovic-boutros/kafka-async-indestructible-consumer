package bzh.lboutros.consumer.runner;

import bzh.lboutros.consumer.offset.ConsumerOffsets;
import bzh.lboutros.consumer.offset.ConsumerOffsets.PartitionOffsetAndMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class SafeConsumerFunctions {
    public static boolean safePause(Consumer<?, ?> consumer, Set<TopicPartition> partitions) {
        try {
            consumer.pause(partitions);
            return true;
        } catch (Exception e) {
            // Something happened here, let's just warn for now, it should be reprocessed later
            log.warn("Error while pausing partitions...", e);
            return false;
        }
    }

    public static boolean safeResume(Consumer<?, ?> consumer, Set<TopicPartition> partitions, ConsumerOffsets offsets) {
        try {
            consumer.resume(partitions.stream()
                    // Filter out revoked partitions
                    .filter(tp -> {
                        PartitionOffsetAndMetadata offsetForPartition = offsets.getOffsetForPartition(tp);
                        return offsetForPartition != null && !offsetForPartition.isRevoked();
                    })
                    .collect(Collectors.toSet()));
            return true;
        } catch (Exception e) {
            // Something happened here, let's just warn for now, it should be reprocessed later
            log.warn("Error while resuming partitions...", e);
            return false;
        }
    }

    public static boolean safeCommit(Consumer<?, ?> consumer, Set<TopicPartition> partitions, ConsumerOffsets offsets) {
        Map<TopicPartition, OffsetAndMetadata> toBeCommitted = partitions.stream()
                // Filter out revoked partitions
                .filter(tp -> {
                    PartitionOffsetAndMetadata offsetForPartition = offsets.getOffsetForPartition(tp);
                    return offsetForPartition != null && !offsetForPartition.isRevoked();
                })
                .collect(Collectors.toMap(Function.identity(),
                        tp -> offsets.getOffsetForPartition(tp).getOffsetAndMetadata()
                ));

        try {
            consumer.commitSync(toBeCommitted);
            return true;
        } catch (Exception e) {
            // Something happened here, let's just warn for now, it should be reprocessed later
            log.warn("Error while committing offsets...", e);
            return false;
        }
    }
}
