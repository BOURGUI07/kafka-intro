package com.example.reactive_kafka.sec12;

import io.netty.util.internal.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.lang.management.ThreadInfo;
import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaConsumerV2 {

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
                .concatMap(KafkaConsumerV2::process)
                .subscribe();

    }
    /*
        In this case, even in the case of error, we won't disconnect from Kafka.
     */
    private static Mono<Void> process(ReceiverRecord<Object,Object> record) {
        return Mono.just(record)
                .doOnNext(r-> {
                    var index = ThreadLocalRandom.current().nextInt(0, 100);
                    log.info("Key: {}, Index: {},Value {} ", r.key(), index,r.value().toString().toCharArray()[index]);
                })
                .retryWhen(Retry.fixedDelay(3,Duration.ofSeconds(1))
                        .onRetryExhaustedThrow((spec,signal) -> signal.failure()) //Get the original failure instead of ExhaustiveRetryException
                )
                .doOnError(ex -> log.info("ERROR MESSAGE: {}",ex.getMessage()))
                .doFinally(s -> record.receiverOffset().acknowledge())
                .onErrorComplete()
                .then();
    }

}
