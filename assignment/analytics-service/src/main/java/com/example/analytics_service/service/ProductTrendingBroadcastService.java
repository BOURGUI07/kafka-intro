package com.example.analytics_service.service;

import com.example.analytics_service.dto.ProductTrendingDTO;
import com.example.analytics_service.repo.ProductViewRepo;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

@Service
@RequiredArgsConstructor
public class ProductTrendingBroadcastService{
    private final ProductViewRepo repo;
    private Flux<List<ProductTrendingDTO>> trends;

    @PostConstruct
    public void init(){
        trends = repo.findTop5ByOrderByCountDesc()
                .map(x->new ProductTrendingDTO(x.getId(), x.getCount()))
                .collectList()
                .filter(Predicate.not(List::isEmpty))
                .repeatWhen(l->l.delayElements(Duration.ofSeconds(3)))
                .distinctUntilChanged()
                .cache(1);
    }
    public Flux<List<ProductTrendingDTO>> getProductTrending() {
        return trends;
    }
}
