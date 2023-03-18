use std::fmt::Debug;
use std::io::{Read, Write};
use bytes::Bytes;
use kafka_encode::KafkaEncodable;
use kafka_encode::primitives::{Array, CompactArray, CompactString, UnsignedVarInt32};
use kafka_encode_derive::KafkaEncodable;
use anyhow::{anyhow, Result};
use crate::protocol::api_key::ApiKey;
use crate::protocol::{ApiVersion, KafkaRequest, KafkaResponse};
use crate::protocol::err::ErrorCode;
use crate::protocol::tags::TaggedFields;

// requests
#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsRequestV0V2 {
}

impl KafkaRequest for ApiVersionsRequestV0V2 {
    fn get_api_key() -> ApiKey {
        return ApiKey::ApiVersions;
    }

    fn get_version() -> ApiVersion {
        2
    }

    fn get_response_header_version() -> ApiVersion {
        return 0;
    }
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsRequestV3 {
    pub client_software_name: CompactString,
    pub client_software_version: CompactString,
    pub tag_buffer: TaggedFields
}

impl KafkaRequest for ApiVersionsRequestV3 {
    fn get_api_key() -> ApiKey {
        return ApiKey::ApiVersions;
    }

    fn get_version() -> ApiVersion {
        3
    }

    fn get_response_header_version() -> ApiVersion {
        0
    }
}

// earlier versions of the response
#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsResponseV0 {
    pub error_code: ErrorCode,
    pub api_keys: Array<SupportedApiKeyVersionsV0V2>
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct SupportedApiKeyVersionsV0V2 {
    pub api_key: ApiKey,
    pub min_version: ApiVersion,
    pub max_version: ApiVersion
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsResponseV1V2 {
    pub error_code: ErrorCode,
    pub api_keys: Array<SupportedApiKeyVersionsV0V2>,
    pub throttle_time_ms: i32
}

// latest version of the response
#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct ApiVersionsResponseV3 {
    pub error_code: ErrorCode,
    pub api_keys: CompactArray<SupportedApiKeyVersionsV3>,
    pub throttle_time_ms: i32,
    pub tag_buffer: ApiVersionsResponseV3TaggedFields
}

impl KafkaResponse for ApiVersionsResponseV3 {
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct SupportedApiKeyVersionsV3 {
    pub api_key: ApiKey,
    pub min_version: ApiVersion,
    pub max_version: ApiVersion,
    pub tag_buffer: TaggedFields
}

#[derive(Debug, Eq, KafkaEncodable, PartialEq, Clone)]
#[kafka_encodable_tagged_fields]
pub struct ApiVersionsResponseV3TaggedFields {
    pub supported_features: Option<CompactArray<SupportedFeatureKeyV3>>,
    pub finalized_features_epoch: Option<i64>,
    pub finalized_features: Option<CompactArray<FinalizedFeatureKeyV3>>
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct SupportedFeatureKeyV3 {
    pub name: String,
    pub min_version: i16,
    pub max_version: i16,
    pub tag_buffer: TaggedFields
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct FinalizedFeatureKeyV3 {
    pub name: String,
    pub min_version: i16,
    pub max_version: i16,
    pub tag_buffer: TaggedFields
}
