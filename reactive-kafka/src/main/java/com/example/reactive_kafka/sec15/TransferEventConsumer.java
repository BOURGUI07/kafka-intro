package com.example.reactive_kafka.sec15;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@RequiredArgsConstructor
public class TransferEventConsumer {
    private final KafkaReceiver<String, String> receiver;

    //1:a,b,10
    //key:fromAccount, toAccount, amount
    private TransferEvent toTransferEvent(ReceiverRecord<String,String> record) {
        var arr = record.value().split(",");
        var runnable = record.key().equals("6")? fail():acknowledge(record);
        return new TransferEvent(
                record.key(),
                arr[0],
                arr[1],
                arr[2],
                runnable
        );
    }

    public Flux<TransferEvent> receive() {
        return receiver.receive()
                .doOnNext(r->log.info("Key: {}, Value: {}", r.key(), r.value()))
                .map(this::toTransferEvent);
    }

    private Runnable acknowledge(ReceiverRecord<String,String> record){
        return () -> record.receiverOffset().acknowledge();
    }

    private Runnable fail(){
        return () -> {
            throw new RuntimeException("Error While Acknowledge");

        };
    }
}
