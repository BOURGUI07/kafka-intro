package com.example.reactive_kafka.sec14;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
//import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaConsumer {

    public static void main(String[] args) {
        var consumerConfig = Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
              //  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"
        );

        var options = ReceiverOptions.<String,Integer>create(consumerConfig)
         //       .withKeyDeserializer(errorHandlingDeserializer())
                .subscription(List.of("order-events")); //topic names

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r-> log.info("Key: {}, Value {} ", r.key(), r.value()))
                .doOnNext(r-> r.receiverOffset().acknowledge())
                .subscribe();
    }

   // private static ErrorHandlingDeserializer<Integer> errorHandlingDeserializer(){
   //     var deserializer = new ErrorHandlingDeserializer<>(new IntegerDeserializer());
   //     deserializer.setFailedDeserializationFunction(
  //              info -> {
    //                log.error("failed record: {}", new String(info.getData()));
    //                return -10_000;
     //           }
   ////     );
  //      return deserializer;
 //   }


}
