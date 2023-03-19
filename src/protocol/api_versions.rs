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

#[cfg(test)]
mod tests {
    use kafka_encode::primitives::{CompactArray, CompactString, VarArray};
    use crate::protocol::api_key::ApiKey::*;
    use crate::protocol::api_versions::{ApiVersionsRequestV3, ApiVersionsResponseV3, ApiVersionsResponseV3TaggedFields, SupportedApiKeyVersionsV3};
    use crate::protocol::err::ErrorCode;
    use crate::protocol::networking::test_request_and_response;
    use crate::protocol::tags::TaggedFields;

    #[test]
    fn test_api_versions_request_v3() {
        let request: ApiVersionsRequestV3 = ApiVersionsRequestV3 {
            client_software_name: CompactString(String::from("kafkart")),
            client_software_version: CompactString(String::from("0.0.1")),
            tag_buffer: TaggedFields {
                tags: VarArray(Vec::new())
            }
        };

        let expected_response = ApiVersionsResponseV3 {
            error_code: ErrorCode::None,
            api_keys: CompactArray(vec![
                SupportedApiKeyVersionsV3 {
                    api_key: Produce,
                    min_version: 0,
                    max_version: 9,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: Fetch,
                    min_version: 0,
                    max_version: 13,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: ListOffsets,
                    min_version: 0,
                    max_version: 7,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: Metadata,
                    min_version: 0,
                    max_version: 12,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                }, SupportedApiKeyVersionsV3 {
                    api_key: LeaderAndIsr,
                    min_version: 0,
                    max_version: 6,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: StopReplica,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: UpdateMetadata,
                    min_version: 0,
                    max_version: 7,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: ControlledShutdown,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: OffsetCommit,
                    min_version: 0,
                    max_version: 8,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: OffsetFetch,
                    min_version: 0,
                    max_version: 8,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: FindCoordinator,
                    min_version: 0,
                    max_version: 4,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: JoinGroup,
                    min_version: 0,
                    max_version: 9,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: Heartbeat,
                    min_version: 0,
                    max_version: 4,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                }, SupportedApiKeyVersionsV3 {
                    api_key: LeaveGroup,
                    min_version: 0,
                    max_version: 5,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: SyncGroup,
                    min_version: 0,
                    max_version: 5,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeGroups,
                    min_version: 0,
                    max_version: 5,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: ListGroups,
                    min_version: 0,
                    max_version: 4,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: SaslHandshake,
                    min_version: 0,
                    max_version: 1,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: ApiVersions,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: CreateTopics,
                    min_version: 0,
                    max_version: 7,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DeleteTopics,
                    min_version: 0,
                    max_version: 6,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DeleteRecords,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: InitProducerId,
                    min_version: 0,
                    max_version: 4,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: OffsetForLeaderEpoch,
                    min_version: 0,
                    max_version: 4,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AddPartitionsToTxn,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AddOffsetsToTxn,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: EndTxn,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: WriteTxnMarkers,
                    min_version: 0,
                    max_version: 1,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: TxnOffsetCommit,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeAcls,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: CreateAcls,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DeleteAcls,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeConfigs,
                    min_version: 0,
                    max_version: 4,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AlterConfigs,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AlterReplicaLogDirs,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeLogDirs,
                    min_version: 0,
                    max_version: 4,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: SaslAuthenticate,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                }, SupportedApiKeyVersionsV3 {
                    api_key: CreatePartitions,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: CreateDelegationToken,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: RenewDelegationToken,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: ExpireDelegationToken,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeDelegationToken,
                    min_version: 0,
                    max_version: 3,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DeleteGroups,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: ElectLeaders,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: IncrementalAlterConfigs,
                    min_version: 0,
                    max_version: 1,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AlterPartitionReassignments,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: ListPartitionReassignments,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: OffsetDelete,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeClientQuotas,
                    min_version: 0,
                    max_version: 1,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AlterClientQuotas,
                    min_version: 0,
                    max_version: 1,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeUserScramCredentials,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AlterUserScramCredentials,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AlterPartition,
                    min_version: 0,
                    max_version: 2,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: UpdateFeatures,
                    min_version: 0,
                    max_version: 1,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeCluster,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeProducers,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: DescribeTransactions,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: ListTransactions,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                },
                SupportedApiKeyVersionsV3 {
                    api_key: AllocateProducerIds,
                    min_version: 0,
                    max_version: 0,
                    tag_buffer: TaggedFields { tags: VarArray(vec![]) }
                }
            ]),
            throttle_time_ms: 0,
            tag_buffer: ApiVersionsResponseV3TaggedFields {
                supported_features: None,
                finalized_features_epoch: Some(0),
                finalized_features: None
            }
        };
        test_request_and_response(request, expected_response);
    }
}