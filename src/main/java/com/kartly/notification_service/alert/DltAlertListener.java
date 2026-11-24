package com.kartly.notification_service.alert;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class DltAlertListener {

    @KafkaListener(
            topics = "order-events-dlt",
            groupId = "notification-dlt-alerts",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    public void onParkedMessage(ConsumerRecord<String, String> record) {
        log.error("DLT ALERT: message parked from topic {} (dlt offset {}). Reason: {}. Payload: {}",
                header(record, "kafka_dlt-original-topic"),
                record.offset(),
                header(record, "kafka_dlt-exception-message"),
                record.value());
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? "unknown" : new String(h.value(), StandardCharsets.UTF_8);
    }
}