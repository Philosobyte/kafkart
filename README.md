# kafkart
This is a nascent library aiming eventually to provide a Rust Kafka client which achieves feature parity with
the Java Kafka client. It has a long way to go and it is nowhere near functional yet. 

# structure
### the `kafka-encode` crate
This crate contains Kafka network protocol primitives as defined in the "Protocol Types" section of 
Kafka's protocol guide (https://kafka.apache.org/protocol.html#protocol_types), such as 
NULLABLE_STRING and COMPACT_ARRAY. It also defines the `KafkaEncodable` trait, which contains 
`to_kafka_bytes` and `from_kafka_bytes` methods for serializing and deserializing from wire protocol bytes. 
The crate contains implementations for all the Kafka protocol's primitive types.

### the `kafka-encode-derive` crate
This crate adds `KafkaEncodable` as a derive macro. Any struct containing Kafka protocol primitives 
can be annotated like `#[derive(KafkaEncodable)]`, at which point `to_kafka_bytes` and `from_kafka_bytes` 
method implementations will be generated for the struct.

### the `kafkart` crate
The main crate currently contains Kafka protocol messages as defined in the "The Messages" section of 
Kafka's protocol guide (https://kafka.apache.org/protocol.html#protocol_messages), such as 
ApiVersions and Produce. It contains a simple networking client, `SingleUseKafkaNetworkingClient`, 
for integration testing purposes, but there is a lot of work left for making a usable consumer and producer.
