package com.example.analytics_service.controller;

import com.example.analytics_service.dto.ProductTrendingDTO;
import com.example.analytics_service.service.ProductTrendingBroadcastService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
@RequestMapping("/trending")
@RequiredArgsConstructor
public class ProductViewController {
    private final ProductTrendingBroadcastService service;

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<List<ProductTrendingDTO>> getTrending() {
        return service.getProductTrending();
    }
}
