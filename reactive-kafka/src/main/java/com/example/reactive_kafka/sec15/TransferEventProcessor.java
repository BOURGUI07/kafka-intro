package com.example.reactive_kafka.sec15;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.function.Predicate;

@Slf4j
@RequiredArgsConstructor
public class TransferEventProcessor {
    private final KafkaSender<String, String> sender;

    public Flux<SenderResult<String>> process(Flux<TransferEvent> flux){
        return flux
                .concatMap(this::validate)
                .concatMap(this::sendTransaction);


    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent event){
        var senderRecords = this.toSenderRecord(event);
        var manager = sender.transactionManager();
        return manager.begin()
                .thenMany(sender.send(senderRecords))
                .concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.fromRunnable(event.acknowledge())))
                .concatWith(manager.commit())
                .last()
                .doOnError(ex -> log.info("Error: {}",ex.getMessage()))
                .onErrorResume(ex ->manager.abort());
    }

    //key 5, doesn't have enough money
    private Mono<TransferEvent> validate(TransferEvent event){
        return Mono.just(event)
                .filter(Predicate.not(e->e.key().equals("5")))
                .switchIfEmpty(
                        Mono.<TransferEvent>fromRunnable(event.acknowledge())
                                .doFirst(() -> log.info("Fails Validation :{}",event.key()))
                );
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecord(TransferEvent event){
        var producerRecord1= new ProducerRecord<>("transaction-events",event.key(), event.to()+ "+"+ event.amount());
        var producerRecord2= new ProducerRecord<>("transaction-events",event.key(), event.from()+ "-"+ event.amount());
        var senderRecord1 = SenderRecord.create(producerRecord1,producerRecord1.key());
        var senderRecord2 = SenderRecord.create(producerRecord2,producerRecord2.key());
        return Flux.just(senderRecord1,senderRecord2);
    }
}
