package com.nttdata.bernal.credit_service.model;

import java.math.BigDecimal;
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
@Document(collection = "credit_products")
public class CreditProduct {

    @Id
    private String id;

    private String customerId;
    private CustomerType customerType;
    private CreditType type;
    private BigDecimal creditLimit;
    private BigDecimal usedAmount;
}
