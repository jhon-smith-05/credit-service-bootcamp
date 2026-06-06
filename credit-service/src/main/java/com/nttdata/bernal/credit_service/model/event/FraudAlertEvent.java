package com.nttdata.bernal.credit_service.model.event;

import lombok.*;

import java.time.LocalDateTime;

@Data
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudAlertEvent {
    private String alertId;
    private String customerId;
    private String creditProductId;
    private String alertType;
    private Double amount;
    private String severity;
    private LocalDateTime timestamp;
}
