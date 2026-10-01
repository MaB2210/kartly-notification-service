package com.kartly.notification_service.consumer;

import com.kartly.notification_service.dto.OrderStatusEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderEventConsumer {
    @KafkaListener(topics = "order-events", groupId = "notification-service-group")
    public void handleOrderStatus(OrderStatusEvent event){
        if("CONFIRMED".equals(event.getStatus())){
            log.info("Sending order confirmation to user {} for order {}, total: {}", event.getUserId(), event.getOrderId(), event.getTotalAmount());
        } else if ("PAYMENT_FAILED".equals(event.getStatus())){
            log.info("Sending payment failure notice to user {} for order {}",event.getUserId(),event.getOrderId());
        }

    }

}
