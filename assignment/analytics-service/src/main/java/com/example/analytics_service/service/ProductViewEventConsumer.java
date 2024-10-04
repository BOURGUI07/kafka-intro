package com.example.analytics_service.service;

import com.example.analytics_service.entity.ProductViewCount;
import com.example.analytics_service.event.ProductViewEvent;
import com.example.analytics_service.repo.ProductViewRepo;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProductViewEventConsumer {
    private final ReactiveKafkaConsumerTemplate<String, ProductViewEvent> template;
    private final ProductViewRepo repo;

    @PostConstruct
    public void subscribe() {
        template
                .receive()
                .bufferTimeout(1000, Duration.ofSeconds(1)) // either give me 1000 events or whatever you collected in those secs
                .flatMap(this::process)
                .subscribe();

    }

    private Mono<Void> process(List<ReceiverRecord<String,ProductViewEvent>> events) {
        var eventsMap = events
                .stream()
                .map(record ->record.value().productId())
                .collect(Collectors.groupingBy(
                        Function.identity(), //group by productId
                        Collectors.counting()

                ));
        return repo.findAllById(eventsMap.keySet())
                .collectMap(ProductViewCount::getId)
                .defaultIfEmpty(Collections.emptyMap())
                .map(dbMap -> eventsMap.keySet().stream().map(productId->updateViewCount(dbMap, eventsMap,productId)).collect(Collectors.toList()))
                .flatMapMany(repo::saveAll)
                .doOnComplete(() ->events.getLast().receiverOffset().acknowledge())
                .doOnError(ex->log.error(ex.getMessage()))
                .then();
    }

    private ProductViewCount updateViewCount(Map<Integer,ProductViewCount> dbMap, Map<Integer,Long> eventsMap, Integer productId) {
        var productViewCount = dbMap.getOrDefault(productId,new ProductViewCount(productId,0L,true));
        productViewCount.setCount(productViewCount.getCount() + eventsMap.get(productId));
        return productViewCount;

    }

}
