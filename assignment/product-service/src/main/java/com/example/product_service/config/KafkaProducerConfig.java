package com.example.product_service.config;

import com.example.product_service.event.ProductViewEvent;
import com.example.product_service.service.ProductViewEventProducer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.core.publisher.Sinks;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    private final Map<String,Object> producerConfig = Map.<String,Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class
    );
    @Bean
    public SenderOptions<String, ProductViewEvent> senderOptions(){
        return SenderOptions.create(producerConfig);
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, ProductViewEvent> producerTemplate(SenderOptions<String, ProductViewEvent> senderOptions){
        return new ReactiveKafkaProducerTemplate<>(senderOptions);
    }

}
