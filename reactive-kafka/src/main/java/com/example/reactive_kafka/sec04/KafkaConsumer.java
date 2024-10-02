package com.example.reactive_kafka.sec04;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Arrays;
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
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events")); //topic names

        KafkaReceiver.create(options)
                .receive()
                .take(3) // Requesting Only 3 messages
                .doOnNext(r-> log.info("Key: {}, Value {} ", r.key(), r.value()))
                .doOnNext(x->x.headers().forEach(h->log.info("Header Key: {}, Header Value/ {}",h.key(), new String(h.value()))))
                .doOnNext(r-> r.receiverOffset().acknowledge())
                .subscribe();

    }
}
