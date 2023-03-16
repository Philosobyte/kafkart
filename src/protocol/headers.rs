use kafka_encode::primitives::{NullableString, VarArray};
use kafka_encode::{KafkaDecodable, KafkaEncodable};
use kafka_encode_derive::{KafkaDecodable, KafkaEncodable};
use crate::protocol::api_key::ApiKey;
use crate::protocol::{ApiVersion};
use crate::protocol::api_key::ApiKey::{ApiVersions, Produce};
use crate::protocol::tags::TaggedFields;

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct RequestHeaderV0 {
    pub request_api_key: ApiKey,
    pub request_api_version: ApiVersion,
    pub correlation_id: i32
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct RequestHeaderV1 {
    pub request_api_key: ApiKey,
    pub request_api_version: ApiVersion,
    pub correlation_id: i32,
    pub client_id: NullableString
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct RequestHeaderV2 {
    pub request_api_key: ApiKey,
    pub request_api_version: ApiVersion,
    pub correlation_id: i32,
    pub client_id: NullableString,
    pub tag_buffer: TaggedFields
}

pub trait ResponseHeader {
    fn get_correlation_id(&self) -> i32;
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ResponseHeaderV0 {
    pub correlation_id: i32
}

impl ResponseHeader for ResponseHeaderV0 {
    fn get_correlation_id(&self) -> i32 {
        self.correlation_id
    }
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ResponseHeaderV1 {
    pub correlation_id: i32,
    pub tag_buffer: TaggedFields
}

impl ResponseHeader for ResponseHeaderV1 {
    fn get_correlation_id(&self) -> i32 {
        self.correlation_id
    }
}



#[test]
fn test_request_header_v0() {
    let mut request_header_bytes: Vec<u8> = vec![ 0, 18, 0, 3, 0, 0, 0, 0];

    let deserialized_request_header = RequestHeaderV0::from_kafka_bytes(&mut &*request_header_bytes).unwrap();
    let expected_request_header: RequestHeaderV0 = RequestHeaderV0 {
        request_api_key: ApiVersions,
        request_api_version: 3,
        correlation_id: 0
    };
    println!("deserialized_request_header: {:?}", deserialized_request_header);

    assert_eq!(deserialized_request_header, expected_request_header);
}

#[test]
fn test_request_header_v1() {
    let mut request_header_bytes: Vec<u8> = vec![ 0, 18, 0, 3, 0, 0, 0, 0, 0, 10, 112, 114, 111, 100, 117, 99, 101, 114, 45, 49];

    let deserialized_request_header = RequestHeaderV1::from_kafka_bytes(&mut &*request_header_bytes).unwrap();
    let expected_request_header: RequestHeaderV1 = RequestHeaderV1 {
        request_api_key: ApiVersions,
        request_api_version: 3,
        correlation_id: 0,
        client_id: NullableString(Some(String::from("producer-1")))
    };
    println!("deserialized_request_header: {:?}", deserialized_request_header);

    assert_eq!(deserialized_request_header, expected_request_header);
}

#[test]
fn test_request_header_v2() {
    let mut request_header_bytes: Vec<u8> = vec![ 0, 18, 0, 3, 0, 0, 0, 0, 0, 10, 112, 114, 111, 100, 117, 99, 101, 114, 45, 49, 0];

    let deserialized_request_header = RequestHeaderV2::from_kafka_bytes(&mut &*request_header_bytes).unwrap();
    let expected_request_header: RequestHeaderV2 = RequestHeaderV2 {
        request_api_key: ApiVersions,
        request_api_version: 3,
        correlation_id: 0,
        client_id: NullableString(Some(String::from("producer-1"))),
        tag_buffer: TaggedFields { tags: VarArray(Vec::new()) }
    };
    println!("deserialized_request_header: {:?}", deserialized_request_header);

    assert_eq!(deserialized_request_header, expected_request_header);
}
