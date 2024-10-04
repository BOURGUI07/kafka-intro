package com.example.product_service.service;

import com.example.product_service.event.ProductViewEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.sender.SenderRecord;

@RequiredArgsConstructor
@Slf4j
@Component
public class ProductViewEventProducer implements InitializingBean {
    private final ReactiveKafkaProducerTemplate<String, ProductViewEvent> template;
    private final Sinks.Many<ProductViewEvent> sink = Sinks.many().unicast().<ProductViewEvent>onBackpressureBuffer();
    private final Flux<ProductViewEvent> flux = sink.asFlux();
    private final String topic = "product-view-events";

    @Override
    public void afterPropertiesSet() {
        subscribe();  // This will be called once the bean is fully initialized.
    }

    public void subscribe() {
        var senderRecordFlux = flux
                .map(event -> new ProducerRecord<>(topic, event.getProductId().toString(), event))
                .map(producerRecord -> SenderRecord.create(producerRecord, producerRecord.key()));
        template.send(senderRecordFlux)
                .doOnNext(record -> log.info("EMITTED EVENT: {}", record.correlationMetadata()))
                .subscribe();
    }

    public void emitEvent(ProductViewEvent productViewEvent) {
        sink.tryEmitNext(productViewEvent);
    }
}

