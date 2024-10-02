package com.example.reactive_kafka.sec06;

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
        When you start consumer1, he's gonna assigned all partitions,0 1, and 2.

        when you start consumer2, he's gonna assigned partition 2.
        Now consumer1 is assigned partitions 0 and 1.

        when you start consumer3, he's gonna assigned partition 2.
        Now consumer2 is assigned partitions 1.
        Now consumer1 is assigned partitions 0.

        if you don't like this behavior, you have to configure assignment strategy
        see KafkaConsumer class now.

        After adding this to consumerConfig
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()
        (The default is RangeAssignor)

        When you start consumer1, he's gonna assigned all partitions 0, 1, and 2.

        when you start consumer2, he's gonna assigned partition 2.
        Now consumer1 is assigned partitions 0 and 1.

        when you start consumer3, he's gonna assigned partition 1.
        Now consumer2 is assigned partitions 2.
        Now consumer1 is assigned partitions 0.
     */
}
