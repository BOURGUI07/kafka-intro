package com.example.reactive_kafka.sec10;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaConsumer {

    public static void main(String[] args) {
        var consumerConfig = Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3 //DEFAULT is 500 // for every batch, we receive 3 items
        );

        var options = ReceiverOptions.create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(List.of("order-events")); //topic names

        KafkaReceiver.create(options)
                .receiveAutoAck()
                .log()
                .flatMap(KafkaConsumer::batchProcess)
                .subscribe();
        /*
            with flatMap(), the consumer subscribes to all flux publishers at the same time
            by default the consumer using flatMap(), it subscribes to 256 publishers at the same time
         */
    }

    public static Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> flux ) {
            return flux
                    .publishOn(Schedulers.boundedElastic())//doing the batch processing on a different thread pool
                    .doFirst(() -> log.info("----------------------------"))
                    .doOnNext(r-> log.info("Key: {}, Value {} ", r.key(), r.value()))
                    .then(Mono.delay(Duration.ofSeconds(1)))
                    .then();
    }

}
