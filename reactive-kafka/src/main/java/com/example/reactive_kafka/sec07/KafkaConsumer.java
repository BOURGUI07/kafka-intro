package com.example.reactive_kafka.sec07;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

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
                .addAssignListener(c->{
                    c.forEach(r->log.info("ASSIGNED POSITION: {}",r.position()));
             //       c.forEach(r->r.seek(r.position()-2)); //I wanna see the last 2 emitted items from all partitions
                    c.stream()
                            .filter(r->r.topicPartition().partition()==2)
                            .findFirst()
                            .ifPresent(r->r.seek(r.position()-2)); // I'm only interested in the last 2 items from partition #2
                })
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .take(3) // Requesting Only 3 messages
                .doOnNext(r-> log.info("Key: {}, Value {} ", r.key(), r.value()))
                .doOnNext(r-> r.receiverOffset().acknowledge())
                .subscribe();
    }
}
