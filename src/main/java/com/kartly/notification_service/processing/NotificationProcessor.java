package com.kartly.notification_service.processing;

import com.kartly.notification_service.dto.OrderStatusEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class NotificationProcessor {

    private final JdbcTemplate jdbc;

    public NotificationProcessor(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public boolean process(OrderStatusEvent event) {
        String eventId = event.getOrderId() + ":" + event.getStatus();

        int inserted = jdbc.update(
                "INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT (event_id) DO NOTHING",
                eventId);
        if (inserted == 0) {
            log.info("Duplicate event {} skipped", eventId);
            return false;
        }

        if ("CONFIRMED".equals(event.getStatus())) {
            log.info("Sending order confirmation to user {} for order {}, total: {}",
                    event.getUserId(), event.getOrderId(), event.getTotalAmount());
        } else if ("PAYMENT_FAILED".equals(event.getStatus())) {
            log.info("Sending payment failure notice to user {} for order {}",
                    event.getUserId(), event.getOrderId());
        }
        return true;
    }
}