package com.kartly.notification_service.eos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ExactlyOnceDemoService {

    private static final String IN = "eos-in";
    private static final String OUT = "eos-out";

    private final String bootstrapServers;

    public ExactlyOnceDemoService(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, Object> run(boolean failBeforeCommit) {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "eos-demo-group",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kartly-eos-demo",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            producer.initTransactions();
            consumer.subscribe(List.of(IN));

            ConsumerRecords<String, String> records = ConsumerRecords.empty();
            for (int i = 0; i < 3 && records.isEmpty(); i++) {
                records = consumer.poll(Duration.ofSeconds(3));
            }
            if (records.isEmpty()) {
                return Map.of("processed", 0, "result", "nothing to read");
            }

            producer.beginTransaction();
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            int processed = 0;
            for (ConsumerRecord<String, String> record : records) {
                producer.send(new ProducerRecord<>(OUT, record.key(), record.value().toUpperCase()));
                offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
                processed++;
            }
            producer.flush();

            if (failBeforeCommit) {
                producer.abortTransaction();
                return Map.of("processed", processed, "result", "aborted before commit");
            }

            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            producer.commitTransaction();
            return Map.of("processed", processed, "result", "committed");
        }
    }
}