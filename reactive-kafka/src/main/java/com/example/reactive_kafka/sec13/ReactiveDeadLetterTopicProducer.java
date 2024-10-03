package com.example.reactive_kafka.sec13;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class ReactiveDeadLetterTopicProducer<K,V> {
    private final KafkaSender<K,V> sender;
    private final Retry retrySpec;

    public Mono<SenderResult<K>> produce(ReceiverRecord<K,V> record) {
        var senderRecord = toSenderRecord(record);
        return sender.send(Mono.just(senderRecord)).next();
    }

    private SenderRecord<K,V,K> toSenderRecord(ReceiverRecord<K,V> record) {
        var producerRecord = new ProducerRecord<>(
                record.topic() + "-DLT",
                record.key(),
                record.value()
        );
        return SenderRecord.create(producerRecord,producerRecord.key());
    }

    public Function<Mono<ReceiverRecord<K,V>>, Mono<Void>> recordProcessingErrorHandler() {
        return mono -> mono
                .retryWhen(this.retrySpec)
                .onErrorMap(ex->ex.getCause() instanceof RejectedExecutionException,Throwable::getCause)
                .doOnError(ex-> log.info("ERROR MESSAGE: {}",ex.getMessage()))
                .onErrorResume(RecordProcessingException.class, ex ->
                        this.produce(ex.getRecord())
                                .then(Mono.fromRunnable(()->ex.getRecord().receiverOffset().acknowledge()))
                )
                .then();
    }
}
