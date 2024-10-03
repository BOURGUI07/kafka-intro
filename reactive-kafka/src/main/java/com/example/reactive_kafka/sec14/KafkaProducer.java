package com.example.reactive_kafka.sec14;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

@Slf4j
public class KafkaProducer {
    public static void start(String[] args) {
        var producerConfig = Map.<String,Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class
        );
        var options = SenderOptions.<String,Integer>create(producerConfig);

        var flux = Flux.range(1,100)
                        .map(i-> new ProducerRecord<>("order-events",i.toString(),i))
                .map(producerRecord -> SenderRecord.create(producerRecord,producerRecord.key()));

        var sender = KafkaSender.create(options);
        sender.send(flux)
                .doOnNext(result->log.info("CORRELATION ID: {}",result.correlationMetadata())) //confirm the record was sent
                .doOnComplete(sender::close)
                .subscribe();
    }
}
