# create a kafka topic called hello-world
# we assume that directory which contains 'kafka-topics.sh' is included in the PATH
kafka-topics.sh --bootstrap-server localhost:9092 --topic hello-world --create

# list all topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# describe a topic
kafka-topics.sh --bootstrap-server localhost:9092 --topic hello-world --describe

# delete a topic
kafka-topics.sh --bootstrap-server localhost:9092 --topic hello-world --delete

# topic with partitions
kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create --partitions 2

# change the number of partitions of a topic
# before changing the number of partitions, make sure to stop the producer, drain all existing partitions, the start the producer again
# if you don't wanna have downtime, create a new topic with the number of partitions desired
# after processing the old messages by the consumers, make them consume the messages of the new topic
kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --alter --partitions 4

# topic with replicaiton factor
kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create --replication-factor 3