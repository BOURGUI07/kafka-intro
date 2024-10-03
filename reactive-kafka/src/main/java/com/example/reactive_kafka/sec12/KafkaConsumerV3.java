package com.example.reactive_kafka.sec12;

import io.netty.util.internal.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaConsumerV3 {

    public static void main(String[] args) {
        var consumerConfig = Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8081",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events")); //topic names

        KafkaReceiver.create(options)
                .receive()
                .concatMap(KafkaConsumerV3::process)
                .subscribe();

    }

    private static Mono<Void> process(ReceiverRecord<Object,Object> record) {
        return Mono.just(record)
                .doOnNext(r-> {
                    if(r.key().toString().equals("5")){
                        throw new RuntimeException("DB IS DOWN");
                    }
                    var index = ThreadLocalRandom.current().nextInt(0, 20);
                    log.info("Key: {}, Index: {},Value {} ", r.key(), index,r.value().toString().toCharArray()[index]);
                    r.receiverOffset().acknowledge();
                })
                .retryWhen(retrySpec())
                .doOnError(ex -> log.info("ERROR MESSAGE: {}",ex.getMessage()))
                .onErrorResume(IndexOutOfBoundsException.class,ex->Mono.fromRunnable(()->record.receiverOffset().acknowledge()))
           //     .doFinally(s -> record.receiverOffset().acknowledge())
            //    .onErrorComplete()
                .then();
    }

    private static Retry retrySpec(){
        return Retry.fixedDelay(3,Duration.ofSeconds(1))
                .filter(IndexOutOfBoundsException.class::isInstance)
                .onRetryExhaustedThrow((spec,signal) -> signal.failure());
    }

}
