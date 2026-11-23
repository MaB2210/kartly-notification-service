package com.kartly.notification_service.replay;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class DltReplayService {

    private static final String DLT_TOPIC = "order-events-dlt";
    private static final String MAIN_TOPIC = "order-events";
    private static final String REPLAY_HEADER = "x-replay-count";
    private static final int MAX_REPLAYS = 3;

    private final String bootstrapServers;

    public DltReplayService(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, Integer> replay(int max) {
        int replayed = 0;
        int parked = 0;

        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "notification-dlt-replay",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max);
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {

            List<TopicPartition> partitions = consumer.partitionsFor(DLT_TOPIC).stream()
                    .map(p -> new TopicPartition(DLT_TOPIC, p.partition()))
                    .toList();
            consumer.assign(partitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions));
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata c = committed.get(tp);
                if (c == null) {
                    consumer.seekToBeginning(List.of(tp));
                } else {
                    consumer.seek(tp, c.offset());
                }
            }

            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();

            while (replayed + parked < max && !caughtUp(consumer, partitions, endOffsets)) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, byte[]> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    if (record.offset() >= endOffsets.get(tp)) {
                        continue;
                    }
                    int count = replayCount(record);
                    if (count >= MAX_REPLAYS) {
                        log.warn("Parking message at DLT offset {} after {} replays", record.offset(), count);
                        parked++;
                    } else {
                        ProducerRecord<String, byte[]> out =
                                new ProducerRecord<>(MAIN_TOPIC, record.key(), record.value());
                        out.headers().add(REPLAY_HEADER,
                                String.valueOf(count + 1).getBytes(StandardCharsets.UTF_8));
                        producer.send(out).get(5, TimeUnit.SECONDS);
                        log.info("Replayed DLT offset {} to {} (replay {})", record.offset(), MAIN_TOPIC, count + 1);
                        replayed++;
                    }
                    toCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                }
                if (!toCommit.isEmpty()) {
                    consumer.commitSync(toCommit);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("DLT replay failed: " + e.getMessage(), e);
        }
        return Map.of("replayed", replayed, "parked", parked);
    }

    private boolean caughtUp(KafkaConsumer<String, byte[]> consumer,
                             List<TopicPartition> partitions,
                             Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition tp : partitions) {
            if (consumer.position(tp) < endOffsets.get(tp)) {
                return false;
            }
        }
        return true;
    }

    private int replayCount(ConsumerRecord<String, byte[]> record) {
        Header header = record.headers().lastHeader(REPLAY_HEADER);
        return header == null ? 0 : Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
    }
}