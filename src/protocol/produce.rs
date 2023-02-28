use std::io::{Read, Write};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes::buf::Reader;
use tracing::info;
use kafka_encode::primitives::{Array, CompactArray, CompactNullableArray, CompactNullableString, CompactString, NullableArray, NullableString, VarArray};
use kafka_encode::{KafkaDecodable, KafkaEncodable};
use kafka_encode_derive::{KafkaDecodable, KafkaEncodable};
use crate::protocol::api_key::ApiKey;
use crate::protocol::api_versions::{ApiVersionsRequestV3, ApiVersionsResponseV3};
use crate::protocol::headers::{RequestHeaderV2, ResponseHeaderV1};
use crate::protocol::records::Record;
use crate::protocol::tags::TaggedFields;

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct PartitionDataV0V8 {
    pub index: i32,
    records: NullableArray<Record>
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct PartitionDataV9 {
    pub index: i32,
    pub records: CompactNullableArray<Record>,
    pub tag_buffer: TaggedFields
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct TopicDataV0V8 {
    pub name: String,
    partition_data: Array<PartitionDataV0V8>
}


#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct TopicDataV9 {
    pub name: CompactString,
    pub partition_data: CompactArray<PartitionDataV9>,
    pub tag_buffer: TaggedFields
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ProduceRequestV0V2 {
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Array<TopicDataV0V8>
}

pub type ProduceRequestV1 = ProduceRequestV0V2;
pub type ProduceRequestV2 = ProduceRequestV0V2;

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ProduceRequestV3V8 {
    pub transactional_id: NullableString,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Array<TopicDataV0V8>
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ProduceRequestV9 {
    pub transactional_id: CompactNullableString,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: CompactArray<PartitionDataV0V8>,
    pub tag_buffer: TaggedFields
}

#[test]
fn test_stuff() {
    info!("try this");
    let mut request_bytes: Reader<Bytes> = Bytes::from(vec![0, 0, 0, 46, 0, 18, 0, 3, 0, 0, 0, 0, 0, 10, 112, 114, 111, 100, 117, 99, 101, 114, 45, 49, 0, 18, 97, 112, 97, 99, 104, 101, 45, 107, 97, 102, 107, 97, 45, 106, 97, 118, 97, 6, 51, 46, 51, 46, 49, 0])
        .reader();
    let length: i32 = i32::from_kafka_bytes(&mut request_bytes).unwrap();
    let request_header: RequestHeaderV2 = RequestHeaderV2::from_kafka_bytes(&mut request_bytes).unwrap();
    println!("length: {:?}, request_header: {:?}", length, request_header);
    let request: ApiVersionsRequestV3 = ApiVersionsRequestV3::from_kafka_bytes(&mut request_bytes).unwrap();
    println!("request: {:?}", request);


    let mut response_bytes: Reader<Bytes> = Bytes::from(vec![0, 0, 0, 0, 0, 0, 0, 60, 0, 0, 0, 0, 0, 9, 0, 0, 1, 0, 0, 0, 13, 0, 0, 2, 0, 0, 0, 7, 0, 0, 3, 0, 0, 0, 12, 0, 0, 4, 0, 0, 0, 6, 0, 0, 5, 0, 0, 0, 3, 0, 0, 6, 0, 0, 0, 7, 0, 0, 7, 0, 0, 0, 3, 0, 0, 8, 0, 0, 0, 8, 0, 0, 9, 0, 0, 0, 8, 0, 0, 10, 0, 0, 0, 4, 0, 0, 11, 0, 0, 0, 9, 0, 0, 12, 0, 0, 0, 4, 0, 0, 13, 0, 0, 0, 5, 0, 0, 14, 0, 0, 0, 5, 0, 0, 15, 0, 0, 0, 5, 0, 0, 16, 0, 0, 0, 4, 0, 0, 17, 0, 0, 0, 1, 0, 0, 18, 0, 0, 0, 3, 0, 0, 19, 0, 0, 0, 7, 0, 0, 20, 0, 0, 0, 6, 0, 0, 21, 0, 0, 0, 2, 0, 0, 22, 0, 0, 0, 4, 0, 0, 23, 0, 0, 0, 4, 0, 0, 24, 0, 0, 0, 3, 0, 0, 25, 0, 0, 0, 3, 0, 0, 26, 0, 0, 0, 3, 0, 0, 27, 0, 0, 0, 1, 0, 0, 28, 0, 0, 0, 3, 0, 0, 29, 0, 0, 0, 3, 0, 0, 30, 0, 0, 0, 3, 0, 0, 31, 0, 0, 0, 3, 0, 0, 32, 0, 0, 0, 4, 0, 0, 33, 0, 0, 0, 2, 0, 0, 34, 0, 0, 0, 2, 0, 0, 35, 0, 0, 0, 4, 0, 0, 36, 0, 0, 0, 2, 0, 0, 37, 0, 0, 0, 3, 0, 0, 38, 0, 0, 0, 3, 0, 0, 39, 0, 0, 0, 2, 0, 0, 40, 0, 0, 0, 2, 0, 0, 41, 0, 0, 0, 3, 0, 0, 42, 0, 0, 0, 2, 0, 0, 43, 0, 0, 0, 2, 0, 0, 44, 0, 0, 0, 1, 0, 0, 45, 0, 0, 0, 0, 0, 0, 46, 0, 0, 0, 0, 0, 0, 47, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 1, 0, 0, 49, 0, 0, 0, 1, 0, 0, 50, 0, 0, 0, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 56, 0, 0, 0, 2, 0, 0, 57, 0, 0, 0, 1, 0, 0, 60, 0, 0, 0, 0, 0, 0, 61, 0, 0, 0, 0, 0, 0, 65, 0, 0, 0, 0, 0, 0, 66, 0, 0, 0, 0, 0, 0, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0 ])
        .reader();
    println!("bytes remaining: {:?}", response_bytes.get_ref().remaining());
    let response_header: ResponseHeaderV1 = ResponseHeaderV1::from_kafka_bytes(&mut response_bytes).unwrap();
    println!("response_header: {:?}", response_header);
    println!("bytes remaining: {:?}", response_bytes.get_ref().remaining());
    let response: ApiVersionsResponseV3 = ApiVersionsResponseV3::from_kafka_bytes(&mut response_bytes).unwrap();
    println!("response: {:?}", response);
}

#[test]
fn test_serialization_stuff() {
    let request_header: RequestHeaderV2 = RequestHeaderV2 {
        request_api_key: ApiKey::JoinGroup,
        request_api_version: 3,
        correlation_id: 7,
        client_id: NullableString(Some(String::from("lol"))),
        tag_buffer: TaggedFields { tags: VarArray(Vec::new()) },
    };
    let ApiVersionsRequestV3 = ApiVersionsRequestV3 {
        client_software_name: CompactString(String::from("lol")),
        client_software_version: CompactString(String::from("lol")),
        tag_buffer: TaggedFields { tags: VarArray(Vec::new()) },
    };
}
