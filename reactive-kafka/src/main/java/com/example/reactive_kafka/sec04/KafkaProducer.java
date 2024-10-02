package com.example.reactive_kafka.sec04;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

@Slf4j
public class KafkaProducer {
    public static void main(String[] args) {
        var producerConfig = Map.<String,Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
                                // key type, value type
        var options = SenderOptions.<String,String>create(producerConfig);

        var flux = Flux.range(1,10)
                .map(KafkaProducer::createSenderRecord);


        var sender = KafkaSender.create(options);
        sender.send(flux)
                .doOnNext(result->log.info("CORRELATION ID: {}",result.correlationMetadata())) //confirm the record was sent
                .doOnComplete(sender::close)
                .subscribe();
    }

    private static SenderRecord<String,String,String> createSenderRecord(Integer i){
        var headers = new RecordHeaders();
        headers.add("client-id","something".getBytes());
        headers.add("tracing-id","1.0".getBytes());
        var producerRecord = new ProducerRecord<>("order-events",null, i.toString(),"order-"+i, headers);
        return SenderRecord.create(producerRecord,producerRecord.key());
    }
}
