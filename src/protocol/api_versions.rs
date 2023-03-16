use std::fmt::Debug;
use std::io::{Read, Write};
use bytes::Bytes;
use kafka_encode::{KafkaEncodable, KafkaDecodable};
use kafka_encode::primitives::{Array, CompactArray, CompactString, UnsignedVarInt32};
use kafka_encode_derive::{KafkaDecodable, KafkaEncodable};
use anyhow::{anyhow, Result};
use crate::protocol::api_key::ApiKey;
use crate::protocol::{ApiVersion, KafkaRequest, KafkaResponse};
use crate::protocol::err::ErrorCode;
use crate::protocol::tags::TaggedFields;

// requests
#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
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

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
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
    pub tag_buffer: ApiVersionsResponseV3TaggedFields
}

impl KafkaResponse for ApiVersionsResponseV3 {
}

#[derive(Debug, KafkaEncodable, KafkaDecodable, Eq, PartialEq, Clone)]
pub struct SupportedApiKeyVersionsV3 {
    pub api_key: ApiKey,
    pub min_version: ApiVersion,
    pub max_version: ApiVersion,
    pub tag_buffer: TaggedFields
}

#[derive(Debug, Eq, KafkaEncodable, KafkaDecodable, PartialEq, Clone)]
#[kafka_encodable_tagged_fields]
#[kafka_decodable_tagged_fields]
pub struct ApiVersionsResponseV3TaggedFields {
    pub supported_features: Option<CompactArray<SupportedFeatureKeyV3>>,
    pub finalized_features_epoch: Option<i64>,
    pub finalized_features: Option<CompactArray<FinalizedFeatureKeyV3>>
}

// impl KafkaEncodable for ApiVersionsResponseV3TaggedFields {
//     fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
//         let mut num_tagged_fields = 0;
//         match self.supported_features {
//             Some(_) => num_tagged_fields += 1,
//             None => {}
//         };
//         match self.finalized_features_epoch {
//             Some(_) => num_tagged_fields += 1,
//             None => {}
//         };
//         match self.finalized_features {
//             Some(_) => num_tagged_fields += 1,
//             None => {}
//         };
//         num_tagged_fields.to_kafka_bytes(writer)?;
//
//         match self.supported_features {
//             Some(supported_features_unwrapped) => {
//                 let tag: UnsignedVarInt32 = UnsignedVarInt32(0);
//                 let mut buffer: Vec<u8> = Vec::new();
//                 supported_features_unwrapped.to_kafka_bytes(&mut buffer)?;
//                 let size: UnsignedVarInt32 = UnsignedVarInt32(buffer.len() as u32);
//
//                 tag.to_kafka_bytes(writer)?;
//                 size.to_kafka_bytes(writer)?;
//                 buffer.to_kafka_bytes(writer)?;
//             },
//             None => {}
//         };
//         match self.finalized_features_epoch {
//             Some(finalized_features_epoch_unwrapped) => {
//                 let tag: UnsignedVarInt32 = UnsignedVarInt32(1);
//                 let mut buffer: Vec<u8> = Vec::new();
//                 finalized_features_epoch_unwrapped.to_kafka_bytes(&mut buffer)?;
//                 let size: UnsignedVarInt32 = UnsignedVarInt32(buffer.len() as u32);
//
//                 tag.to_kafka_bytes(writer)?;
//                 size.to_kafka_bytes(writer)?;
//                 buffer.to_kafka_bytes(writer)?;
//             },
//             None => {}
//         };
//         match self.finalized_features {
//             Some(finalized_features_unwrapped) => {
//                 let tag: UnsignedVarInt32 = UnsignedVarInt32(2);
//                 let mut buffer: Vec<u8> = Vec::new();
//                 finalized_features_unwrapped.to_kafka_bytes(&mut buffer)?;
//                 let size: UnsignedVarInt32 = UnsignedVarInt32(buffer.len() as u32);
//
//                 tag.to_kafka_bytes(writer)?;
//                 size.to_kafka_bytes(writer)?;
//                 buffer.to_kafka_bytes(writer)?;
//             },
//             None => {}
//         };
//         Ok(())
//     }
// }

// impl KafkaDecodable for ApiVersionsResponseV3TaggedFields {
//     fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<Self> {
//         let num_tagged_fields = UnsignedVarInt32::from_kafka_bytes(reader)?;
//
//         let mut supported_features: Option<CompactArray<SupportedFeatureKeyV3>> = None;
//         let mut finalized_features_epoch: Option<i64> = None;
//         let mut finalized_features: Option<CompactArray<FinalizedFeatureKeyV3>> = None;
//
//         for i in 0..num_tagged_fields.0 {
//             let tag: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
//             let size: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
//             match tag.0 {
//                  0 => {
//                     match supported_features {
//                         Some(existing_feature) => return Err(anyhow!("feature already exists! {:?}", existing_feature)),
//                         None => {
//                             supported_features = Some(CompactArray::from_kafka_bytes(reader)?);
//                         }
//                     };
//                 },
//                 1 => {
//                     match supported_features {
//                         Some(existing_feature) => return Err(anyhow!("feature already exists! {:?}", existing_feature)),
//                         None => {
//                             finalized_features_epoch = Some(i64::from_kafka_bytes(reader)?);
//                         }
//                     };
//                 },
//                 2 => {
//                     match supported_features {
//                         Some(existing_feature) => return Err(anyhow!("feature already exists! {:?}", existing_feature)),
//                         None => {
//                             finalized_features = Some(CompactArray::from_kafka_bytes(reader)?);
//                         }
//                     };
//                 },
//                 unsupported_feature => {
//                     return Err(anyhow!("Unsupported tagged feature: {:?}", unsupported_feature));
//                 }
//             }
//         }
//
//         return Ok(
//             ApiVersionsResponseV3TaggedFields {
//                 supported_features,
//                 finalized_features_epoch,
//                 finalized_features
//             }
//         );
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
