// use bytes::{Bytes, BytesMut};
// use kafka_encode::{KafkaDecodable, KafkaEncodable};
// use kafka_encode::primitives::CompactNullableArray;
// use kafka_encode_derive::{KafkaDecodable, KafkaEncodable};
//
// #[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
// pub struct StructWithNamedFields {
//     pub string: String,
//     pub integer: i32,
//     pub byte_vec: Vec<u8>,
//     pub compact_string_array: CompactNullableArray<String>
// }
//
// #[test]
// fn test_derive_kafka_encodable_and_kafka_decodable() {
//     let to_serialize = StructWithNamedFields {
//         string: String::from("The quick"),
//         integer: 42,
//         byte_vec: vec![10, 8, 6, 4, 2],
//         compact_string_array: CompactNullableArray(Some(vec![String::from("brown"), String::from("fox")]))
//     };
//
//     let mut write_buffer: BytesMut = BytesMut::new();
//     to_serialize.clone().to_kafka_bytes(&mut write_buffer).unwrap();
//
//     let mut read_buffer: Bytes = write_buffer.freeze();
//     let deserialized = StructWithNamedFields::from_kafka_bytes(&mut read_buffer).unwrap();
//
//     assert_eq!(deserialized, to_serialize);
// }
