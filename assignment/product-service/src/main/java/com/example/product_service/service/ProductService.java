package com.example.product_service.service;

import com.example.product_service.dto.ProductResponse;
import com.example.product_service.event.ProductViewEvent;
import com.example.product_service.mapper.ProductMapper;
import com.example.product_service.repo.ProductRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ProductService {
    private final ProductRepo repo;
    private final ProductMapper mapper;
    private final ProductViewEventProducer eventProducer;

    public Mono<ProductResponse> getProduct(Integer id) {
        return repo.findById(id)
                .doOnNext(x->eventProducer.emitEvent(new ProductViewEvent(x.getId())))
                .map(mapper::toDto);
    }
}
