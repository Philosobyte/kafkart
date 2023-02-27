use std::io::Read;
use bytes::Bytes;
use kafka_encode::{KafkaEncodable, KafkaDecodable};
use kafka_encode::primitives::{Array, CompactArray, CompactString, UnsignedVarInt32};
use kafka_encode_derive::{KafkaDecodable, KafkaEncodable};
use crate::protocol::api_key::ApiKey;
use crate::protocol::ApiVersion;
use crate::protocol::err::ErrorCode;
use crate::protocol::tags::TaggedFields;

// requests
#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsRequestV0V2 {
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsRequestV3 {
    pub client_software_name: CompactString,
    pub client_software_version: CompactString,
    pub tag_buffer: TaggedFields
}

// earlier versions of the response
#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsResponseV0 {
    pub error_code: ErrorCode,
    pub api_keys: Array<SupportedApiKeyVersionsV0V2>
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct SupportedApiKeyVersionsV0V2 {
    pub api_key: ApiKey,
    pub min_version: ApiVersion,
    pub max_version: ApiVersion
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsResponseV1V2 {
    pub error_code: ErrorCode,
    pub api_keys: Array<SupportedApiKeyVersionsV0V2>,
    pub throttle_time_ms: i32
}

// latest version of the response
#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsResponseV3 {
    pub error_code: ErrorCode,
    pub api_keys: CompactArray<SupportedApiKeyVersionsV3>,
    pub throttle_time_ms: i32,
    pub tag_buffer: TaggedFields
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct SupportedApiKeyVersionsV3 {
    pub api_key: ApiKey,
    pub min_version: ApiVersion,
    pub max_version: ApiVersion,
    pub tag_buffer: TaggedFields
}

pub struct ApiVersionsResponseV3TaggedFields {
    pub supported_features: Option<CompactArray<SupportedFeatureKeyV3>>,
    pub finalized_features_epoch: Option<i64>,
    pub finalized_features: Option<CompactArray<FinalizedFeatureKeyV3>>
}

// impl KafkaDecodable for ApiVersionsResponseV3TaggedFields {
//     fn from_kafka_bytes<R: Read>(reader: &mut R) -> std::io::Result<ApiVersionsResponseV3TaggedFields> {
//         let num_tagged_fields = UnsignedVarInt32::from_kafka_bytes(reader);
//         for i in 0..num_tagged_fields {
//             let tag: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
//             let size: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
//             match tag.0 {
//                  0 => {
//
//                 },
//                 1 => {
//
//                 },
//                 2 => {
//
//                 },
//                 _ => {
//
//                 }
//             }
//         }
//     }
// }

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct SupportedFeatureKeyV3 {
    pub name: String,
    pub min_version: i16,
    pub max_version: i16,
    pub tag_buffer: TaggedFields
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct FinalizedFeatureKeyV3 {
    pub name: String,
    pub min_version: i16,
    pub max_version: i16,
    pub tag_buffer: TaggedFields
}

// impl KafkaDecodable for SupportedApiKeyVersionsV3 {
//     fn from_kafka_bytes(read_buffer: &mut Bytes) -> std::io::Result<Self> {
//         let api_key: ApiKey = ApiKey::from_kafka_bytes(read_buffer)?;
//         let min_version: ApiVersion = ApiVersion::from_kafka_bytes(read_buffer)?;
//         let max_version: ApiVersion = ApiVersion::from_kafka_bytes(read_buffer)?;
//         let tag_buffer: TagBuffer = TagBuffer::from_kafka_bytes(read_buffer)?;
//         let supported_api_key_versions_v3 = SupportedApiKeyVersionsV3 {
//             api_key,
//             min_version,
//             max_version,
//             tag_buffer
//         };
//         Ok(supported_api_key_versions_v3)
//     }
// }
