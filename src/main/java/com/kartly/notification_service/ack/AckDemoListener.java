package com.kartly.notification_service.ack;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AckDemoListener {

    @KafkaListener(topics = "ack-demo", groupId = "ack-demo-group", containerFactory = "manualAckFactory")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("ACK DEMO: received '{}' at offset {}", record.value(), record.offset());
        if (record.value().startsWith("skip")) {
            log.warn("ACK DEMO: NOT acknowledging offset {}", record.offset());
            return;
        }
        ack.acknowledge();
        log.info("ACK DEMO: acknowledged offset {}", record.offset());
    }
}