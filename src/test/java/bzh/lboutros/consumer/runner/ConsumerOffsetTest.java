package bzh.lboutros.consumer.runner;


import bzh.lboutros.consumer.offset.ConsumerOffsets;
import bzh.lboutros.consumer.offset.ConsumerOffsets.PartitionOffsetAndMetadata;
import bzh.lboutros.consumer.offset.InfiniteRetriesRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static bzh.lboutros.consumer.runner.SafeConsumerFunctions.*;

public class ConsumerOffsetTest {
    @Test
    public void empty_assignment() {
        // Given
        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerOffsets offsets = new ConsumerOffsets("client-1");
        InfiniteRetriesRebalanceListener listener = new InfiniteRetriesRebalanceListener(consumer, offsets);
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        consumer.subscribe(List.of("topic"), listener);
        consumer.updateBeginningOffsets(Map.of(topicPartition, 0L));

        // When
        consumer.rebalance(List.of(topicPartition));
        listener.onPartitionsAssigned(consumer.assignment());

        // Then
        PartitionOffsetAndMetadata offsetForPartition = offsets.getOffsetForPartition(topicPartition);
        Assertions.assertNull(offsetForPartition);
    }

    @Test
    public void first_offset() {
        // Given
        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerOffsets offsets = new ConsumerOffsets("client-1");
        InfiniteRetriesRebalanceListener listener = new InfiniteRetriesRebalanceListener(consumer, offsets);
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        consumer.subscribe(List.of("topic"), listener);
        consumer.updateBeginningOffsets(Map.of(topicPartition, 0L));

        // When
        consumer.rebalance(List.of(topicPartition));
        listener.onPartitionsAssigned(consumer.assignment());
        consumer.addRecord(new ConsumerRecord<>("topic", 0, 0L, "key", "value"));
        ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(10));
        records.forEach(r -> offsets.incrementOffsetPosition(r));
        safeCommit(consumer, consumer.assignment(), offsets);

        // Then
        PartitionOffsetAndMetadata offsetForPartition = offsets.getOffsetForPartition(topicPartition);
        Assertions.assertNotNull(offsetForPartition);
        Assertions.assertEquals(1L, offsetForPartition.getOffsetAndMetadata().offset());
    }

    @Test
    public void reassignment_between_poll_and_pause() {
        // Given
        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerOffsets offsets = new ConsumerOffsets("client-1");
        InfiniteRetriesRebalanceListener listener = new InfiniteRetriesRebalanceListener(consumer, offsets);
        TopicPartition topicPartition0 = new TopicPartition("topic", 0);
        TopicPartition topicPartition1 = new TopicPartition("topic", 1);
        consumer.subscribe(List.of("topic"), listener);
        consumer.updateBeginningOffsets(Map.of(topicPartition0, 0L));
        consumer.updateBeginningOffsets(Map.of(topicPartition1, 0L));

        // When
        consumer.rebalance(List.of(topicPartition0, topicPartition1));
        listener.onPartitionsAssigned(consumer.assignment());
        consumer.addRecord(new ConsumerRecord<>("topic", 0, 0L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 0L, "key2", "value"));
        ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(10));
        records.forEach(r -> offsets.incrementOffsetPosition(r));
        safeCommit(consumer, consumer.assignment(), offsets);

        consumer.addRecord(new ConsumerRecord<>("topic", 0, 1L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 1L, "key2", "value"));
        records = consumer.poll(Duration.ofMillis(10));

        consumer.rebalance(List.of(topicPartition1));
        listener.onPartitionsRevoked(List.of(topicPartition0));
        listener.onPartitionsAssigned(consumer.assignment());

        boolean successPausedConsumer = safePause(consumer, records.partitions());

        // Then
        Assertions.assertEquals(2, records.partitions().size());
        Assertions.assertEquals(1, consumer.assignment().size());
        Assertions.assertEquals(1, consumer.assignment().iterator().next().partition());
        Assertions.assertFalse(successPausedConsumer);

        PartitionOffsetAndMetadata offsetForPartition0 = offsets.getOffsetForPartition(topicPartition0);
        Assertions.assertNotNull(offsetForPartition0);
        Assertions.assertEquals(1L, offsetForPartition0.getOffsetAndMetadata().offset());
        Assertions.assertTrue(offsetForPartition0.isRevoked());
    }

    @Test
    public void reassignment_between_pause_and_increment_offsets() {
        // Given
        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerOffsets offsets = new ConsumerOffsets("client-1");
        InfiniteRetriesRebalanceListener listener = new InfiniteRetriesRebalanceListener(consumer, offsets);
        TopicPartition topicPartition0 = new TopicPartition("topic", 0);
        TopicPartition topicPartition1 = new TopicPartition("topic", 1);
        consumer.subscribe(List.of("topic"), listener);
        consumer.updateBeginningOffsets(Map.of(topicPartition0, 0L));
        consumer.updateBeginningOffsets(Map.of(topicPartition1, 0L));

        // When
        consumer.rebalance(List.of(topicPartition0, topicPartition1));
        listener.onPartitionsAssigned(consumer.assignment());
        consumer.addRecord(new ConsumerRecord<>("topic", 0, 0L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 0L, "key2", "value"));
        ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(10));
        records.forEach(r -> offsets.incrementOffsetPosition(r));
        safeCommit(consumer, consumer.assignment(), offsets);

        consumer.addRecord(new ConsumerRecord<>("topic", 0, 1L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 1L, "key2", "value"));
        records = consumer.poll(Duration.ofMillis(10));
        boolean successPausedConsumer = safePause(consumer, records.partitions());

        consumer.rebalance(List.of(topicPartition1));
        listener.onPartitionsRevoked(List.of(topicPartition0));
        listener.onPartitionsAssigned(consumer.assignment());

        records.forEach(offsets::incrementOffsetPosition);

        boolean successCommittedConsumer = safeCommit(consumer, records.partitions(), offsets);
        boolean successResumedConsumer = safeResume(consumer, records.partitions(), offsets);

        // Then
        Assertions.assertEquals(2, records.partitions().size());
        Assertions.assertEquals(1, consumer.assignment().size());
        Assertions.assertEquals(1, consumer.assignment().iterator().next().partition());

        Assertions.assertTrue(successPausedConsumer);
        Assertions.assertTrue(successCommittedConsumer);
        Assertions.assertTrue(successResumedConsumer);

        PartitionOffsetAndMetadata offsetForPartition0 = offsets.getOffsetForPartition(topicPartition0);
        Assertions.assertNotNull(offsetForPartition0);
        Assertions.assertEquals(1L, offsetForPartition0.getOffsetAndMetadata().offset());
        Assertions.assertTrue(offsetForPartition0.isRevoked());

        PartitionOffsetAndMetadata offsetForPartition1 = offsets.getOffsetForPartition(topicPartition1);
        Assertions.assertNotNull(offsetForPartition1);
        Assertions.assertEquals(2L, offsetForPartition1.getOffsetAndMetadata().offset());
        Assertions.assertFalse(offsetForPartition1.isRevoked());
    }

    @Test
    public void reassignment_between_increment_offsets_and_commit_resume() {
        // Given
        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerOffsets offsets = new ConsumerOffsets("client-1");
        InfiniteRetriesRebalanceListener listener = new InfiniteRetriesRebalanceListener(consumer, offsets);
        TopicPartition topicPartition0 = new TopicPartition("topic", 0);
        TopicPartition topicPartition1 = new TopicPartition("topic", 1);
        consumer.subscribe(List.of("topic"), listener);
        consumer.updateBeginningOffsets(Map.of(topicPartition0, 0L));
        consumer.updateBeginningOffsets(Map.of(topicPartition1, 0L));

        // When
        consumer.rebalance(List.of(topicPartition0, topicPartition1));
        listener.onPartitionsAssigned(consumer.assignment());
        consumer.addRecord(new ConsumerRecord<>("topic", 0, 0L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 0L, "key2", "value"));
        ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(10));
        records.forEach(offsets::incrementOffsetPosition);
        safeCommit(consumer, consumer.assignment(), offsets);

        consumer.addRecord(new ConsumerRecord<>("topic", 0, 1L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 1L, "key2", "value"));
        records = consumer.poll(Duration.ofMillis(10));
        boolean successPausedConsumer = safePause(consumer, records.partitions());

        records.forEach(offsets::incrementOffsetPosition);

        consumer.rebalance(List.of(topicPartition1));
        listener.onPartitionsRevoked(List.of(topicPartition0));
        listener.onPartitionsAssigned(consumer.assignment());

        boolean successCommittedConsumer = safeCommit(consumer, records.partitions(), offsets);
        boolean successResumedConsumer = safeResume(consumer, records.partitions(), offsets);

        // Then
        Assertions.assertEquals(2, records.partitions().size());
        Assertions.assertEquals(1, consumer.assignment().size());
        Assertions.assertEquals(1, consumer.assignment().iterator().next().partition());

        Assertions.assertTrue(successPausedConsumer);
        Assertions.assertTrue(successCommittedConsumer);
        Assertions.assertTrue(successResumedConsumer);

        PartitionOffsetAndMetadata offsetForPartition0 = offsets.getOffsetForPartition(topicPartition0);
        Assertions.assertNotNull(offsetForPartition0);
        // The offset is 2 here because the reassignment occurred and the offset was not updated for this revoked partition
        Assertions.assertEquals(2L, offsetForPartition0.getOffsetAndMetadata().offset());
        Assertions.assertTrue(offsetForPartition0.isRevoked());

        PartitionOffsetAndMetadata offsetForPartition1 = offsets.getOffsetForPartition(topicPartition1);
        Assertions.assertNotNull(offsetForPartition1);
        // The offset is 1 here because the reassignment occurred after incrementing offsets but before committing offsets
        Assertions.assertEquals(1L, offsetForPartition1.getOffsetAndMetadata().offset());
        Assertions.assertFalse(offsetForPartition1.isRevoked());
    }

    @Test
    public void reassignment_revoked_partition() {
        // Given
        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerOffsets offsets = new ConsumerOffsets("client-1");
        InfiniteRetriesRebalanceListener listener = new InfiniteRetriesRebalanceListener(consumer, offsets);
        TopicPartition topicPartition0 = new TopicPartition("topic", 0);
        TopicPartition topicPartition1 = new TopicPartition("topic", 1);
        consumer.subscribe(List.of("topic"), listener);
        consumer.updateBeginningOffsets(Map.of(topicPartition0, 0L));
        consumer.updateBeginningOffsets(Map.of(topicPartition1, 0L));

        // When
        consumer.rebalance(List.of(topicPartition0, topicPartition1));
        listener.onPartitionsAssigned(consumer.assignment());
        consumer.addRecord(new ConsumerRecord<>("topic", 0, 0L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 0L, "key2", "value"));
        ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(10));
        records.forEach(offsets::incrementOffsetPosition);
        safeCommit(consumer, consumer.assignment(), offsets);

        consumer.addRecord(new ConsumerRecord<>("topic", 0, 1L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 1L, "key2", "value"));
        records = consumer.poll(Duration.ofMillis(10));
        boolean successPausedConsumer = safePause(consumer, records.partitions());

        consumer.rebalance(List.of(topicPartition1));
        listener.onPartitionsRevoked(List.of(topicPartition0));
        listener.onPartitionsAssigned(consumer.assignment());

        records.forEach(offsets::incrementOffsetPosition);

        boolean successCommittedConsumer = safeCommit(consumer, records.partitions(), offsets);
        boolean successResumedConsumer = safeResume(consumer, records.partitions(), offsets);

        consumer.rebalance(List.of(topicPartition0, topicPartition1));
        listener.onPartitionsAssigned(consumer.assignment());

        // Reinsert the records (Mock consumer remove the records in its list in the poll function)
        consumer.addRecord(new ConsumerRecord<>("topic", 0, 0L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 0L, "key2", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 0, 1L, "key1", "value"));
        consumer.addRecord(new ConsumerRecord<>("topic", 1, 1L, "key2", "value"));

        records = consumer.poll(Duration.ofMillis(10));
        successPausedConsumer = safePause(consumer, records.partitions());

        records.forEach(offsets::incrementOffsetPosition);

        successCommittedConsumer = safeCommit(consumer, records.partitions(), offsets);
        successResumedConsumer = safeResume(consumer, records.partitions(), offsets);

        // Then
        // The second record of the partition 0 is reprocessed after reassignment
        Assertions.assertEquals(1, records.partitions().size());
        Assertions.assertEquals(2, consumer.assignment().size());

        Assertions.assertTrue(successPausedConsumer);
        Assertions.assertTrue(successCommittedConsumer);
        Assertions.assertTrue(successResumedConsumer);

        PartitionOffsetAndMetadata offsetForPartition = offsets.getOffsetForPartition(topicPartition0);
        Assertions.assertNotNull(offsetForPartition);
        Assertions.assertEquals(2L, offsetForPartition.getOffsetAndMetadata().offset());
        Assertions.assertFalse(offsetForPartition.isRevoked());

        PartitionOffsetAndMetadata offsetForPartition1 = offsets.getOffsetForPartition(topicPartition1);
        Assertions.assertNotNull(offsetForPartition1);
        // The offset is 1 here because the reassignment occurred after incrementing offsets but before committing offsets
        Assertions.assertEquals(2L, offsetForPartition1.getOffsetAndMetadata().offset());
        Assertions.assertFalse(offsetForPartition1.isRevoked());
    }
}
