package com.example.reactive_kafka.sec15;

public record TransferEvent(
        String key,
        String from,
        String to,
        String amount,
        Runnable acknowledge
) {
}
