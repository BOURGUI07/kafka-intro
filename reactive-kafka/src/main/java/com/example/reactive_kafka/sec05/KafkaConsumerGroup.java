package com.example.reactive_kafka.sec05;

public class KafkaConsumerGroup {
    private static class consumer1{
        public static void main(String[] args) {
            KafkaConsumer.start("1");
        }
    }

    private static class consumer2{
        public static void main(String[] args) {
            KafkaConsumer.start("2");
        }
    }

    private static class consumer3{
        public static void main(String[] args) {
            KafkaConsumer.start("3");
        }
    }

    /*
        kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --delete
        kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create --partitions 3
     */
}
