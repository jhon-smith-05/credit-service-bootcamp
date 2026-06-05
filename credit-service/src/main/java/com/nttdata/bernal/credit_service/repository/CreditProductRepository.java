package com.nttdata.bernal.credit_service.repository;

import com.nttdata.bernal.credit_service.model.CreditProduct;
import com.nttdata.bernal.credit_service.model.CreditType;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface CreditProductRepository extends ReactiveMongoRepository<CreditProduct, String> {

    Mono<Boolean> existsByCustomerIdAndType(String customerId, CreditType type);
}
