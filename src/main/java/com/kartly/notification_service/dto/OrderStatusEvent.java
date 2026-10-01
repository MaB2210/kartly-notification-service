package com.kartly.notification_service.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
public class OrderStatusEvent {
    private Long orderId;
    private Long userId;
    private BigDecimal totalAmount;
    private String status;
}
