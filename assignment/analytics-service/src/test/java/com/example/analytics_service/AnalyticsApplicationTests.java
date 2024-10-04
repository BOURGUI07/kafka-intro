package com.example.analytics_service;

import com.example.analytics_service.dto.ProductTrendingDTO;
import com.example.analytics_service.event.ProductViewEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootTest
@AutoConfigureWebTestClient
public class AnalyticsApplicationTests extends AbstractIntegrationTest{
    @Autowired
    private WebTestClient client;
    @Test
    void trendingTest() {

        // emit events
        var events = Flux.just(
                        createEvent(2, 2),
                        createEvent(1, 1),
                        createEvent(6, 3),
                        createEvent(4, 2),
                        createEvent(5, 5),
                        createEvent(4, 2),
                        createEvent(6, 3),
                        createEvent(3, 3)
                ).flatMap(Flux::fromIterable)
                .map(e -> this.toSenderRecord(PRODUCT_VIEW_EVENTS, e.productId().toString(), e));

        var resultFlux = this.<ProductViewEvent>createSender().send(events);

        StepVerifier.create(resultFlux)
                .expectNextCount(21)
                .verifyComplete();

        // verify via trending endpoint
        var mono = this.client
                .get()
                .uri("/trending")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .returnResult(new ParameterizedTypeReference<List<ProductTrendingDTO>>() {})
                .getResponseBody()
                .next();

        StepVerifier.create(mono)
                .consumeNextWith(this::validateResult)
                .verifyComplete();

    }

    // 6,5,4,3,2   1
    private void validateResult(List<ProductTrendingDTO> list){
        Assertions.assertEquals(5, list.size());
        Assertions.assertEquals(6, list.get(0).productId());
        Assertions.assertEquals(6, list.get(0).productId());
        Assertions.assertEquals(2, list.get(4).productId());
        Assertions.assertEquals(2, list.get(4).viewCount());
        Assertions.assertTrue(list.stream().noneMatch(p -> p.productId() == 1));
    }

    private List<ProductViewEvent> createEvent(Integer productId, Integer count){
        return IntStream.rangeClosed(1,count)
                .mapToObj(x-> new ProductViewEvent(productId))
                .collect(Collectors.toList());
    }
}
