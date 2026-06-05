package com.nttdata.bernal.credit_service.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "credit_movements")
public class CreditMovement {

    @Id
    private String id;

    private String creditProductId;
    private CreditMovementType type;
    private BigDecimal amount;
    private BigDecimal resultingUsedAmount;
    private LocalDateTime createdAt;
}
