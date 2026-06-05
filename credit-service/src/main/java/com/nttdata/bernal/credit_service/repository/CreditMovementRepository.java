package com.nttdata.bernal.credit_service.repository;

import com.nttdata.bernal.credit_service.model.CreditMovement;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;

public interface CreditMovementRepository extends ReactiveMongoRepository<CreditMovement, String> {

    Flux<CreditMovement> findByCreditProductId(String creditProductId);
}
