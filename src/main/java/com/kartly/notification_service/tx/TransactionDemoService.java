package com.kartly.notification_service.tx;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class TransactionDemoService {

    private final String bootstrapServers;

    public TransactionDemoService(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, String> run(boolean abort) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kartly-tx-demo");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("tx-demo-a", "k", "message A"));
            producer.send(new ProducerRecord<>("tx-demo-b", "k", "message B"));
            producer.flush();
            log.info("TX DEMO: both messages flushed to the broker, now {}", abort ? "aborting" : "committing");
            if (abort) {
                producer.abortTransaction();
                return Map.of("result", "aborted");
            }
            producer.commitTransaction();
            return Map.of("result", "committed");
        }
    }
}