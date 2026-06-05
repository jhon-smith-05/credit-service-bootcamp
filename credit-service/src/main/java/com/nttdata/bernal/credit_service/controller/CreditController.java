package com.nttdata.bernal.credit_service.controller;

import com.nttdata.bernal.credit_service.model.CreditMovement;
import com.nttdata.bernal.credit_service.model.CreditProduct;
import com.nttdata.bernal.credit_service.service.CreditService;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/credits")
@RequiredArgsConstructor
public class CreditController {

    private final CreditService creditService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Single<CreditProduct> create(@RequestBody CreditProduct creditProduct) {
        return creditService.create(creditProduct);
    }

    @GetMapping
    public Flowable<CreditProduct> findAll() {
        return creditService.findAll();
    }

    @GetMapping("/{id}")
    public Single<CreditProduct> findById(@PathVariable("id") String id) {
        return creditService.findById(id);
    }

    @PutMapping("/{id}")
    public Single<CreditProduct> update(@PathVariable("id") String id, @RequestBody CreditProduct creditProduct) {
        return creditService.update(id, creditProduct);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Completable delete(@PathVariable("id") String id) {
        return creditService.delete(id);
    }

    @PostMapping("/{id}/payments")
    public Single<CreditMovement> pay(@PathVariable("id") String id, @RequestBody Map<String, BigDecimal> request) {
        return creditService.pay(id, request.get("amount"));
    }

    @PostMapping("/{id}/charges")
    public Single<CreditMovement> charge(@PathVariable("id") String id, @RequestBody Map<String, BigDecimal> request) {
        return creditService.charge(id, request.get("amount"));
    }

    @GetMapping("/{id}/available-balance")
    public Single<Map<String, BigDecimal>> getAvailableBalance(@PathVariable("id") String id) {
        return creditService.getAvailableBalance(id).map(balance -> Map.of("availableBalance", balance));
    }

    @GetMapping("/{id}/movements")
    public Flowable<CreditMovement> findMovements(@PathVariable("id") String id) {
        return creditService.findMovements(id);
    }
}
