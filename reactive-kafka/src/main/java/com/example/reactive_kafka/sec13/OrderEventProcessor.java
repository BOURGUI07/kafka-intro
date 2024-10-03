package com.example.reactive_kafka.sec13;

import io.netty.util.internal.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;

@RequiredArgsConstructor
@Slf4j
public class OrderEventProcessor {
    private final ReactiveDeadLetterTopicProducer<String,String> producer;

    public Mono<Void> process(ReceiverRecord<String,String> record) {
        return Mono.just(record)
                .doOnNext(r-> {
                    if(r.key().endsWith("5")) throw new RuntimeException("Processing Error");
                    log.info("Key: {}, Value {} ", r.key() ,r.value());
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(ex->new RecordProcessingException(record,ex))
                .transform(producer.recordProcessingErrorHandler())
                .then();
    }
}
