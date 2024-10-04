package com.example.analytics_service.config;

import com.example.analytics_service.event.ProductViewEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    private final Map<String, Object> consumerConfig =
            Map.<String,Object>of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
                    ConsumerConfig.GROUP_ID_CONFIG, "analytics-service",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1",
                    JsonDeserializer.TRUSTED_PACKAGES,"*");

    @Bean
    public ReceiverOptions<String, ProductViewEvent> receiverOptions(){
        return ReceiverOptions.<String, ProductViewEvent>create(consumerConfig)
                .consumerProperty(JsonDeserializer.VALUE_DEFAULT_TYPE, ProductViewEvent.class)
                .consumerProperty(JsonDeserializer.USE_TYPE_INFO_HEADERS, false)
                .subscription(List.of("product-view-events"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, ProductViewEvent> kafkaConsumerTemplate(ReceiverOptions<String, ProductViewEvent> receiverOptions){
        return new ReactiveKafkaConsumerTemplate<>(receiverOptions);
    }
}
