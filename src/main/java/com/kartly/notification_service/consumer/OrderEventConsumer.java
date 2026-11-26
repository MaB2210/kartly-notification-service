package com.kartly.notification_service.consumer;

import com.kartly.notification_service.dto.OrderStatusEvent;
import com.kartly.notification_service.processing.NotificationProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderEventConsumer {

    private final NotificationProcessor processor;

    @Value("${notification.simulate-failure:false}")
    private boolean simulateFailure;

    public OrderEventConsumer(NotificationProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(topics = "order-events", groupId = "notification-service-group")
    public void handleOrderStatus(OrderStatusEvent event) {
        if (simulateFailure) {
            throw new IllegalStateException("Simulated downstream outage");
        }
        if (event.getUserId() == null || event.getOrderId() == null) {
            throw new IllegalArgumentException("Invalid order event: missing userId or orderId");
        }
        processor.process(event);
    }
}