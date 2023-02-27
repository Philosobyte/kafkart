use std::fmt::Debug;
use anyhow::{anyhow, Result};
use std::io::{Error, ErrorKind, Read, Write};
use bytes::{Bytes, BytesMut};
use kafka_encode::{KafkaDecodable, KafkaEncodable};

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
    DescribeClientQuotas = 48,
    AlterClientQuotas = 49,
    DescribeUserScramCredentials = 50,
    AlterUserScramCredentials = 51,
    DescribeQuorum = 55,
    AlterPartition = 56,
    UpdateFeatures = 57,
    DescribeCluster = 60,
    DescribeProducers = 61,
    UnregisterBroker = 64,
    DescribeTransactions = 65,
    ListTransactions = 66,
    AllocateProducerIds = 67,
}
impl TryFrom<i16> for ApiKey {
    type Error = anyhow::Error;
    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            i if i == ApiKey::Produce as i16 => Ok(ApiKey::Produce),
            i if i == ApiKey::Fetch as i16 => Ok(ApiKey::Fetch),
            i if i == ApiKey::ListOffsets as i16 => Ok(ApiKey::ListOffsets),
            i if i == ApiKey::Metadata as i16 => Ok(ApiKey::Metadata),
            i if i == ApiKey::LeaderAndIsr as i16 => Ok(ApiKey::LeaderAndIsr),
            i if i == ApiKey::StopReplica as i16 => Ok(ApiKey::StopReplica),
            i if i == ApiKey::UpdateMetadata as i16 => Ok(ApiKey::UpdateMetadata),
            i if i == ApiKey::ControlledShutdown as i16 => Ok(ApiKey::ControlledShutdown),
            i if i == ApiKey::OffsetCommit as i16 => Ok(ApiKey::OffsetCommit),
            i if i == ApiKey::OffsetFetch as i16 => Ok(ApiKey::OffsetFetch),
            i if i == ApiKey::FindCoordinator as i16 => Ok(ApiKey::FindCoordinator),
            i if i == ApiKey::JoinGroup as i16 => Ok(ApiKey::JoinGroup),
            i if i == ApiKey::Heartbeat as i16 => Ok(ApiKey::Heartbeat),
            i if i == ApiKey::LeaveGroup as i16 => Ok(ApiKey::LeaveGroup),
            i if i == ApiKey::SyncGroup as i16 => Ok(ApiKey::SyncGroup),
            i if i == ApiKey::DescribeGroups as i16 => Ok(ApiKey::DescribeGroups),
            i if i == ApiKey::ListGroups as i16 => Ok(ApiKey::ListGroups),
            i if i == ApiKey::SaslHandshake as i16 => Ok(ApiKey::SaslHandshake),
            i if i == ApiKey::ApiVersions as i16 => Ok(ApiKey::ApiVersions),
            i if i == ApiKey::CreateTopics as i16 => Ok(ApiKey::CreateTopics),
            i if i == ApiKey::DeleteTopics as i16 => Ok(ApiKey::DeleteTopics),
            i if i == ApiKey::DeleteRecords as i16 => Ok(ApiKey::DeleteRecords),
            i if i == ApiKey::InitProducerId as i16 => Ok(ApiKey::InitProducerId),
            i if i == ApiKey::OffsetForLeaderEpoch as i16 => Ok(ApiKey::OffsetForLeaderEpoch),
            i if i == ApiKey::AddPartitionsToTxn as i16 => Ok(ApiKey::AddPartitionsToTxn),
            i if i == ApiKey::AddOffsetsToTxn as i16 => Ok(ApiKey::AddOffsetsToTxn),
            i if i == ApiKey::EndTxn as i16 => Ok(ApiKey::EndTxn),
            i if i == ApiKey::WriteTxnMarkers as i16 => Ok(ApiKey::WriteTxnMarkers),
            i if i == ApiKey::TxnOffsetCommit as i16 => Ok(ApiKey::TxnOffsetCommit),
            i if i == ApiKey::DescribeAcls as i16 => Ok(ApiKey::DescribeAcls),
            i if i == ApiKey::CreateAcls as i16 => Ok(ApiKey::CreateAcls),
            i if i == ApiKey::DeleteAcls as i16 => Ok(ApiKey::DeleteAcls),
            i if i == ApiKey::DescribeConfigs as i16 => Ok(ApiKey::DescribeConfigs),
            i if i == ApiKey::AlterConfigs as i16 => Ok(ApiKey::AlterConfigs),
            i if i == ApiKey::AlterReplicaLogDirs as i16 => Ok(ApiKey::AlterReplicaLogDirs),
            i if i == ApiKey::DescribeLogDirs as i16 => Ok(ApiKey::DescribeLogDirs),
            i if i == ApiKey::SaslAuthenticate as i16 => Ok(ApiKey::SaslAuthenticate),
            i if i == ApiKey::CreatePartitions as i16 => Ok(ApiKey::CreatePartitions),
            i if i == ApiKey::CreateDelegationToken as i16 => Ok(ApiKey::CreateDelegationToken),
            i if i == ApiKey::RenewDelegationToken as i16 => Ok(ApiKey::RenewDelegationToken),
            i if i == ApiKey::ExpireDelegationToken as i16 => Ok(ApiKey::ExpireDelegationToken),
            i if i == ApiKey::DescribeDelegationToken as i16 => Ok(ApiKey::DescribeDelegationToken),
            i if i == ApiKey::DeleteGroups as i16 => Ok(ApiKey::DeleteGroups),
            i if i == ApiKey::ElectLeaders as i16 => Ok(ApiKey::ElectLeaders),
            i if i == ApiKey::IncrementalAlterConfigs as i16 => Ok(ApiKey::IncrementalAlterConfigs),
            i if i == ApiKey::AlterPartitionReassignments as i16 => Ok(ApiKey::AlterPartitionReassignments),
            i if i == ApiKey::ListPartitionReassignments as i16 => Ok(ApiKey::ListPartitionReassignments),
            i if i == ApiKey::OffsetDelete as i16 => Ok(ApiKey::OffsetDelete),
            i if i == ApiKey::DescribeClientQuotas as i16 => Ok(ApiKey::DescribeClientQuotas),
            i if i == ApiKey::AlterClientQuotas as i16 => Ok(ApiKey::AlterClientQuotas),
            i if i == ApiKey::DescribeUserScramCredentials as i16 => Ok(ApiKey::DescribeUserScramCredentials),
            i if i == ApiKey::AlterUserScramCredentials as i16 => Ok(ApiKey::AlterUserScramCredentials),
            i if i == ApiKey::DescribeQuorum as i16 => Ok(ApiKey::DescribeQuorum),
            i if i == ApiKey::AlterPartition as i16 => Ok(ApiKey::AlterPartition),
            i if i == ApiKey::UpdateFeatures as i16 => Ok(ApiKey::UpdateFeatures),
            i if i == ApiKey::DescribeCluster as i16 => Ok(ApiKey::DescribeCluster),
            i if i == ApiKey::DescribeProducers as i16 => Ok(ApiKey::DescribeProducers),
            i if i == ApiKey::UnregisterBroker as i16 => Ok(ApiKey::UnregisterBroker),
            i if i == ApiKey::DescribeTransactions as i16 => Ok(ApiKey::DescribeTransactions),
            i if i == ApiKey::ListTransactions as i16 => Ok(ApiKey::ListTransactions),
            i if i == ApiKey::AllocateProducerIds as i16 => Ok(ApiKey::AllocateProducerIds),
            _ => Err(anyhow!("Unable to determine api key for value: {}", value))
        }
    }
}

impl KafkaEncodable for ApiKey {
    fn to_kafka_bytes<W: Write + Debug>(self, write_buffer: &mut W) -> Result<()> {
        (self as i16).to_kafka_bytes(write_buffer)
    }
}

impl KafkaDecodable for ApiKey {
    fn from_kafka_bytes<R: Read + Debug>(read_buffer: &mut R) -> Result<ApiKey> {
        let api_key: i16 = i16::from_kafka_bytes(read_buffer)?;
        ApiKey::try_from(api_key)
    }
}
