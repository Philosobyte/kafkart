struct KafkaConsumer<'a, K, V> {
    group_id: &'a str,
    client_id: &'a str,

}