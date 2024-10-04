package com.example.reactive_kafka.sec16;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.ReceiverOptions;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate<K,V>;

import java.util.List;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String, String> receiverOptions(KafkaProperties props, SslBundles bundles) {
        return ReceiverOptions.<String,String>create(props.buildConsumerProperties(bundles))
                .subscription(List.of("order-events"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> consumerTemplate(ReceiverOptions<String, String> options){
        return new ReactiveKafkaConsumerTemplate<>(options);
    }
}
