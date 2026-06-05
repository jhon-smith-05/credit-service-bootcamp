package com.nttdata.bernal.credit_service.service;

import com.nttdata.bernal.credit_service.exception.BusinessRuleException;
import com.nttdata.bernal.credit_service.exception.ResourceNotFoundException;
import com.nttdata.bernal.credit_service.model.*;
import com.nttdata.bernal.credit_service.repository.CreditMovementRepository;
import com.nttdata.bernal.credit_service.repository.CreditProductRepository;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;
import reactor.adapter.rxjava.RxJava3Adapter;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@RefreshScope
@RequiredArgsConstructor
public class CreditService {

    private final CreditProductRepository creditProductRepository;
    private final CreditMovementRepository creditMovementRepository;

    public Single<CreditProduct> create(CreditProduct creditProduct) {
        log.debug("Creating credit product {} for customer {}", creditProduct.getType(), creditProduct.getCustomerId());
        Mono<CreditProduct> result = validateCreation(creditProduct)
                .then(creditProductRepository.save(normalize(creditProduct)));
        return RxJava3Adapter.monoToSingle(result);
    }

    public Flowable<CreditProduct> findAll() {
        return RxJava3Adapter.fluxToFlowable(creditProductRepository.findAll());
    }

    public Single<CreditProduct> findById(String id) {
        return RxJava3Adapter.monoToSingle(findCreditProduct(id));
    }

    public Single<CreditProduct> update(String id, CreditProduct creditProduct) {
        Mono<CreditProduct> result = findCreditProduct(id)
                .flatMap(current -> {
                    current.setCreditLimit(creditProduct.getCreditLimit());
                    return creditProductRepository.save(current);
                });
        return RxJava3Adapter.monoToSingle(result);
    }

    public Completable delete(String id) {
        return RxJava3Adapter.monoToCompletable(findCreditProduct(id).flatMap(creditProductRepository::delete));
    }

    public Single<CreditMovement> pay(String id, BigDecimal amount) {
        return registerMovement(id, CreditMovementType.PAYMENT, amount);
    }

    public Single<CreditMovement> charge(String id, BigDecimal amount) {
        return registerMovement(id, CreditMovementType.CHARGE, amount);
    }

    public Single<BigDecimal> getAvailableBalance(String id) {
        return RxJava3Adapter.monoToSingle(findCreditProduct(id)
                .map(product -> product.getCreditLimit().subtract(product.getUsedAmount())));
    }

    public Flowable<CreditMovement> findMovements(String id) {
        return RxJava3Adapter.fluxToFlowable(creditMovementRepository.findByCreditProductId(id));
    }

    private Mono<Void> validateCreation(CreditProduct creditProduct) {
        if (creditProduct.getCustomerType() == CustomerType.PERSONAL
                && creditProduct.getType() == CreditType.PERSONAL_LOAN) {
            return creditProductRepository.existsByCustomerIdAndType(creditProduct.getCustomerId(), CreditType.PERSONAL_LOAN)
                    .flatMap(exists -> exists
                            ? Mono.error(new BusinessRuleException("Personal customers can only have one personal loan"))
                            : Mono.empty());
        }
        if (creditProduct.getCustomerType() == CustomerType.PERSONAL
                && creditProduct.getType() == CreditType.BUSINESS_LOAN) {
            return Mono.error(new BusinessRuleException("Personal customers cannot have business loans"));
        }
        if (creditProduct.getCustomerType() == CustomerType.BUSINESS
                && creditProduct.getType() == CreditType.PERSONAL_LOAN) {
            return Mono.error(new BusinessRuleException("Business customers cannot have personal loans"));
        }
        return Mono.empty();
    }

    private CreditProduct normalize(CreditProduct product) {
        if (product.getCreditLimit() == null) {
            product.setCreditLimit(BigDecimal.ZERO);
        }
        if (product.getUsedAmount() == null) {
            product.setUsedAmount(BigDecimal.ZERO);
        }
        return product;
    }

    private Single<CreditMovement> registerMovement(String id, CreditMovementType type, BigDecimal amount) {
        Mono<CreditMovement> result = findCreditProduct(id)
                .flatMap(product -> validateMovement(product, type, amount)
                        .then(saveMovement(product, type, amount)));
        return RxJava3Adapter.monoToSingle(result);
    }

    private Mono<Void> validateMovement(CreditProduct product, CreditMovementType type, BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            return Mono.error(new BusinessRuleException("Movement amount must be greater than zero"));
        }
        if (type == CreditMovementType.CHARGE
                && product.getUsedAmount().add(amount).compareTo(product.getCreditLimit()) > 0) {
            return Mono.error(new BusinessRuleException("Charge exceeds available credit limit"));
        }
        if (type == CreditMovementType.PAYMENT
                && product.getUsedAmount().compareTo(amount) < 0) {
            return Mono.error(new BusinessRuleException("Payment exceeds current debt"));
        }
        return Mono.empty();
    }

    private Mono<CreditMovement> saveMovement(CreditProduct product, CreditMovementType type, BigDecimal amount) {
        BigDecimal usedAmount = type == CreditMovementType.CHARGE
                ? product.getUsedAmount().add(amount)
                : product.getUsedAmount().subtract(amount);
        product.setUsedAmount(usedAmount);
        return creditProductRepository.save(product)
                .then(creditMovementRepository.save(CreditMovement.builder()
                        .creditProductId(product.getId())
                        .type(type)
                        .amount(amount)
                        .resultingUsedAmount(usedAmount)
                        .createdAt(LocalDateTime.now())
                        .build()));
    }

    private Mono<CreditProduct> findCreditProduct(String id) {
        return creditProductRepository.findById(id)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Credit product not found")));
    }
}
