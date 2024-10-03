package com.example.reactive_kafka.sec13;

import lombok.Getter;
import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException {
    private final ReceiverRecord<?,?> record;

    public RecordProcessingException(ReceiverRecord<?, ?> record, Throwable e) {
        super(e);
        this.record = record;
    }

    @SuppressWarnings("unchecked")
    public <K,V> ReceiverRecord<K, V> getRecord() {
        return (ReceiverRecord<K, V>) record;
    }
}
