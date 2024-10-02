package com.example.reactive_kafka.sec01;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class Lec01KafkaConsumer {
    public static void main(String[] args) {
        var consumerConfig = Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // without this property, the new group won't get all messages from beginning
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events")); //topic names

        KafkaReceiver.create(options)
                .receive()
                .take(3) // Requesting Only 3 messages
                .doOnNext(r-> log.info("Key: {}, Value {} ", r.key(), r.value())) // processing the messages
                .doOnNext(r-> r.receiverOffset().acknowledge()) // acknowledging the messages
                .subscribe();

        /*
            docker exec -it kafka bash
            kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create
            kafka-console-producer.sh --bootstrap-server localhost:9092 --topic order-events
         */

        /*
            Whenever a new group joins, Kafka broker will give him all the messages consumer by old consumers again
            So the older consumer has to tell Kafka it has processed the messages(ACKNOWLEDGE THE MESSAGE) so that old messages won't be sent again
            to a new joining group
         */
    }
}
