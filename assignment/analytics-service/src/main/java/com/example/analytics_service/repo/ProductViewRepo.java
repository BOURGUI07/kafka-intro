package com.example.analytics_service.repo;

import com.example.analytics_service.entity.ProductViewCount;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface ProductViewRepo extends ReactiveCrudRepository<ProductViewCount,Integer> {
    Flux<ProductViewCount> findTop5ByOrderByCountDesc();
}
