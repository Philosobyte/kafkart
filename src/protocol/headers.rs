use kafka_encode::primitives::NullableString;
use kafka_encode::{KafkaDecodable, KafkaEncodable};
use kafka_encode_derive::{KafkaDecodable, KafkaEncodable};
use crate::protocol::api_key::ApiKey;
use crate::protocol::{ApiVersion};
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

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ResponseHeaderV0 {
    pub correlation_id: i32
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ResponseHeaderV1 {
    pub correlation_id: i32,
    pub tag_buffer: TaggedFields
}
