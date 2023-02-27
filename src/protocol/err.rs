use anyhow::{anyhow, Error, Result};
use thiserror::Error;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Write};
use bytes::{Bytes, BytesMut};
use kafka_encode::{KafkaDecodable, KafkaEncodable};

#[derive(Debug, Clone, Error, Eq, PartialEq)]
pub enum ErrorCode {
    #[error("The server experienced an unexpected error when processing the request.")]
    UnknownServerError = -1,
    #[error("This is actually not an error and if this gets printed, mistakes were made.")]
    None = 0,
    #[error("The requested offset is not within the range of offsets maintained by the server.")]
    OffsetOutOfRange = 1,
    #[error("This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.")]
    CorruptMessage = 2,
    #[error("This server does not host this topic-partition.")]
    UnknownTopicOrPartition = 3,
    #[error("The requested fetch size is invalid.")]
    InvalidFetchSize = 4,
    #[error("There is no leader for this topic-partition as we are in the middle of a leadership election.")]
    LeaderNotAvailable = 5,
    #[error("For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.")]
    NotLeaderOrFollower = 6,
    #[error("The request timed out.")]
    RequestTimedOut = 7,
    #[error("The broker is not available.")]
    BrokerNotAvailable = 8,
    #[error("The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.")]
    ReplicaNotAvailable = 9,
    #[error("The request included a message larger than the max message size the server will accept.")]
    MessageTooLarge = 10,
    #[error("The controller moved to another broker.")]
    StaleControllerEpoch = 11,
    #[error("The metadata field of the offset request was too large.")]
    OffsetMetadataTooLarge = 12,
    #[error("The server disconnected before a response was received.")]
    NetworkException = 13,
    #[error("The coordinator is loading and hence can't process requests.")]
    CoordinatorLoadInProgress = 14,
    #[error("The coordinator is not available.")]
    CoordinatorNotAvailable = 15,
    #[error("This is not the correct coordinator.")]
    NotCoordinator = 16,
    #[error("The request attempted to perform an operation on an invalid topic.")]
    InvalidTopicException = 17,
    #[error("The request included message batch larger than the configured segment size on the server.")]
    RecordListTooLarge = 18,
    #[error("Messages are rejected since there are fewer in-sync replicas than required.")]
    NotEnoughReplicas = 19,
    #[error("Messages are written to the log, but to fewer in-sync replicas than required.")]
    NotEnoughReplicasAfterAppend = 20,
    #[error("Produce request specified an invalid value for required acks.")]
    InvalidRequiredAcks = 21,
    #[error("Specified group generation id is not valid.")]
    IllegalGeneration = 22,
    #[error("The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.")]
    InconsistentGroupProtocol = 23,
    #[error("The configured groupId is invalid.")]
    InvalidGroupId = 24,
    #[error("The coordinator is not aware of this member.")]
    UnknownMemberId = 25,
    #[error("The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).")]
    InvalidSessionTimeout = 26,
    #[error("The group is rebalancing, so a rejoin is needed.")]
    RebalanceInProgress = 27,
    #[error("The committing offset data size is not valid.")]
    InvalidCommitOffsetSize = 28,
    #[error("Topic authorization failed.")]
    TopicAuthorizationFailed = 29,
    #[error("Group authorization failed.")]
    GroupAuthorizationFailed = 30,
    #[error("Cluster authorization failed.")]
    ClusterAuthorizationFailed = 31,
    #[error("The timestamp of the message is out of acceptable range.")]
    InvalidTimestamp = 32,
    #[error("The broker does not support the requested SASL mechanism.")]
    UnsupportedSaslMechanism = 33,
    #[error("Request is not valid given the current SASL state.")]
    IllegalSaslState = 34,
    #[error("The version of API is not supported.")]
    UnsupportedVersion = 35,
    #[error("Topic with this name already exists.")]
    TopicAlreadyExists = 36,
    #[error("Number of partitions is below 1.")]
    InvalidPartitions = 37,
    #[error("Replication factor is below 1 or larger than the number of available brokers.")]
    InvalidReplicationFactor = 38,
    #[error("Replica assignment is invalid.")]
    InvalidReplicaAssignment = 39,
    #[error("Configuration is invalid.")]
    InvalidConfig = 40,
    #[error("This is not the correct controller for this cluster.")]
    NotController = 41,
    #[error("This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.")]
    InvalidRequest = 42,
    #[error("The message format version on the broker does not support the request.")]
    UnsupportedForMessageFormat = 43,
    #[error("Request parameters do not satisfy the configured policy.")]
    PolicyViolation = 44,
    #[error("The broker received an out of order sequence number.")]
    OutOfOrderSequenceNumber = 45,
    #[error("The broker received a duplicate sequence number.")]
    DuplicateSequenceNumber = 46,
    #[error("Producer attempted to produce with an old epoch.")]
    InvalidProducerEpoch = 47,
    #[error("The producer attempted a transactional operation in an invalid state.")]
    InvalidTxnState = 48,
    #[error("The producer attempted to use a producer id which is not currently assigned to its transactional id.")]
    InvalidProducerIdMapping = 49,
    #[error("The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).")]
    InvalidTransactionTimeout = 50,
    #[error("The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.")]
    ConcurrentTransactions = 51,
    #[error("Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.")]
    TransactionCoordinatorFenced = 52,
    #[error("Transactional Id authorization failed.")]
    TransactionalIdAuthorizationFailed = 53,
    #[error("Security features are disabled.")]
    SecurityDisabled = 54,
    #[error("The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.")]
    OperationNotAttempted = 55,
    #[error("Disk error when trying to access log file on the disk.")]
    KafkaStorageError = 56,
    #[error("The user-specified log directory is not found in the broker config.")]
    LogDirNotFound = 57,
    #[error("SASL Authentication failed.")]
    SaslAuthenticationFailed = 58,
    #[error("This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.")]
    UnknownProducerId = 59,
    #[error("A partition reassignment is in progress.")]
    ReassignmentInProgress = 60,
    #[error("Delegation Token feature is not enabled.")]
    DelegationTokenAuthDisabled = 61,
    #[error("Delegation Token is not found on server.")]
    DelegationTokenNotFound = 62,
    #[error("Specified Principal is not valid Owner/Renewer.")]
    DelegationTokenOwnerMismatch = 63,
    #[error("Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.")]
    DelegationTokenRequestNotAllowed = 64,
    #[error("Delegation Token authorization failed.")]
    DelegationTokenAuthorizationFailed = 65,
    #[error("Delegation Token is expired.")]
    DelegationTokenExpired = 66,
    #[error("Supplied principalType is not supported.")]
    InvalidPrincipalType = 67,
    #[error("The group is not empty.")]
    NonEmptyGroup = 68,
    #[error("The group id does not exist.")]
    GroupIdNotFound = 69,
    #[error("The fetch session ID was not found.")]
    FetchSessionIdNotFound = 70,
    #[error("The fetch session epoch is invalid.")]
    InvalidFetchSessionEpoch = 71,
    #[error("There is no listener on the leader broker that matches the listener on which metadata request was processed.")]
    ListenerNotFound = 72,
    #[error("Topic deletion is disabled.")]
    TopicDeletionDisabled = 73,
    #[error("The leader epoch in the request is older than the epoch on the broker.")]
    FencedLeaderEpoch = 74,
    #[error("The leader epoch in the request is newer than the epoch on the broker.")]
    UnknownLeaderEpoch = 75,
    #[error("The requesting client does not support the compression type of given partition.")]
    UnsupportedCompressionType = 76,
    #[error("Broker epoch has changed.")]
    StaleBrokerEpoch = 77,
    #[error("The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.")]
    OffsetNotAvailable = 78,
    #[error("The group member needs to have a valid member id before actually entering a consumer group.")]
    MemberIdRequired = 79,
    #[error("The preferred leader was not available.")]
    PreferredLeaderNotAvailable = 80,
    #[error("The consumer group has reached its max size.")]
    GroupMaxSizeReached = 81,
    #[error("The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.")]
    FencedInstanceId = 82,
    #[error("Eligible topic partition leaders are not available.")]
    EligibleLeadersNotAvailable = 83,
    #[error("Leader election not needed for topic partition.")]
    ElectionNotNeeded = 84,
    #[error("No partition reassignment is in progress.")]
    NoReassignmentInProgress = 85,
    #[error("Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.")]
    GroupSubscribedToTopic = 86,
    #[error("This record has failed the validation on broker and hence will be rejected.")]
    InvalidRecord = 87,
    #[error("There are unstable offsets that need to be cleared.")]
    UnstableOffsetCommit = 88,
    #[error("The throttling quota has been exceeded.")]
    ThrottlingQuotaExceeded = 89,
    #[error("There is a newer producer with the same transactionalId which fences the current one.")]
    ProducerFenced = 90,
    #[error("A request illegally referred to a resource that does not exist.")]
    ResourceNotFound = 91,
    #[error("A request illegally referred to the same resource twice.")]
    DuplicateResource = 92,
    #[error("Requested credential would not meet criteria for acceptability.")]
    UnacceptableCredential = 93,
    #[error("Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters")]
    InconsistentVoterSet = 94,
    #[error("The given update version was invalid.")]
    InvalidUpdateVersion = 95,
    #[error("Unable to update finalized features due to an unexpected server error.")]
    FeatureUpdateFailed = 96,
    #[error("Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.")]
    PrincipalDeserializationFailure = 97,
    #[error("Requested snapshot was not found")]
    SnapshotNotFound = 98,
    #[error("Requested position is not greater than or equal to zero, and less than the size of the snapshot.")]
    PositionOutOfRange = 99,
    #[error("This server does not host this topic ID.")]
    UnknownTopicId = 100,
    #[error("This broker ID is already in use.")]
    DuplicateBrokerRegistration = 101,
    #[error("The given broker ID was not registered.")]
    BrokerIdNotRegistered = 102,
    #[error("The log's topic ID did not match the topic ID in the request")]
    InconsistentTopicId = 103,
    #[error("The clusterId in the request does not match that found on the server")]
    InconsistentClusterId = 104,
    #[error("The transactionalId could not be found")]
    TransactionalIdNotFound = 105,
    #[error("The fetch session encountered inconsistent topic ID usage")]
    FetchSessionTopicIdError = 106,
    #[error("The new ISR contains at least one ineligible replica.")]
    IneligibleReplica = 107,
    #[error("The AlterPartition request successfully updated the partition state but the leader has changed.")]
    NewLeaderElected = 108,
}

impl TryFrom<i16> for ErrorCode {
    type Error = anyhow::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            i if i == ErrorCode::UnknownServerError as i16 => Ok(ErrorCode::UnknownServerError),
            i if i == ErrorCode::None as i16 => Ok(ErrorCode::None),
            i if i == ErrorCode::OffsetOutOfRange as i16 => Ok(ErrorCode::OffsetOutOfRange),
            i if i == ErrorCode::CorruptMessage as i16 => Ok(ErrorCode::CorruptMessage),
            i if i == ErrorCode::UnknownTopicOrPartition as i16 => Ok(ErrorCode::UnknownTopicOrPartition),
            i if i == ErrorCode::InvalidFetchSize as i16 => Ok(ErrorCode::InvalidFetchSize),
            i if i == ErrorCode::LeaderNotAvailable as i16 => Ok(ErrorCode::LeaderNotAvailable),
            i if i == ErrorCode::NotLeaderOrFollower as i16 => Ok(ErrorCode::NotLeaderOrFollower),
            i if i == ErrorCode::RequestTimedOut as i16 => Ok(ErrorCode::RequestTimedOut),
            i if i == ErrorCode::BrokerNotAvailable as i16 => Ok(ErrorCode::BrokerNotAvailable),
            i if i == ErrorCode::ReplicaNotAvailable as i16 => Ok(ErrorCode::ReplicaNotAvailable),
            i if i == ErrorCode::MessageTooLarge as i16 => Ok(ErrorCode::MessageTooLarge),
            i if i == ErrorCode::StaleControllerEpoch as i16 => Ok(ErrorCode::StaleControllerEpoch),
            i if i == ErrorCode::OffsetMetadataTooLarge as i16 => Ok(ErrorCode::OffsetMetadataTooLarge),
            i if i == ErrorCode::NetworkException as i16 => Ok(ErrorCode::NetworkException),
            i if i == ErrorCode::CoordinatorLoadInProgress as i16 => Ok(ErrorCode::CoordinatorLoadInProgress),
            i if i == ErrorCode::CoordinatorNotAvailable as i16 => Ok(ErrorCode::CoordinatorNotAvailable),
            i if i == ErrorCode::NotCoordinator as i16 => Ok(ErrorCode::NotCoordinator),
            i if i == ErrorCode::InvalidTopicException as i16 => Ok(ErrorCode::InvalidTopicException),
            i if i == ErrorCode::RecordListTooLarge as i16 => Ok(ErrorCode::RecordListTooLarge),
            i if i == ErrorCode::NotEnoughReplicas as i16 => Ok(ErrorCode::NotEnoughReplicas),
            i if i == ErrorCode::NotEnoughReplicasAfterAppend as i16 => Ok(ErrorCode::NotEnoughReplicasAfterAppend),
            i if i == ErrorCode::InvalidRequiredAcks as i16 => Ok(ErrorCode::InvalidRequiredAcks),
            i if i == ErrorCode::IllegalGeneration as i16 => Ok(ErrorCode::IllegalGeneration),
            i if i == ErrorCode::InconsistentGroupProtocol as i16 => Ok(ErrorCode::InconsistentGroupProtocol),
            i if i == ErrorCode::InvalidGroupId as i16 => Ok(ErrorCode::InvalidGroupId),
            i if i == ErrorCode::UnknownMemberId as i16 => Ok(ErrorCode::UnknownMemberId),
            i if i == ErrorCode::InvalidSessionTimeout as i16 => Ok(ErrorCode::InvalidSessionTimeout),
            i if i == ErrorCode::RebalanceInProgress as i16 => Ok(ErrorCode::RebalanceInProgress),
            i if i == ErrorCode::InvalidCommitOffsetSize as i16 => Ok(ErrorCode::InvalidCommitOffsetSize),
            i if i == ErrorCode::TopicAuthorizationFailed as i16 => Ok(ErrorCode::TopicAuthorizationFailed),
            i if i == ErrorCode::GroupAuthorizationFailed as i16 => Ok(ErrorCode::GroupAuthorizationFailed),
            i if i == ErrorCode::ClusterAuthorizationFailed as i16 => Ok(ErrorCode::ClusterAuthorizationFailed),
            i if i == ErrorCode::InvalidTimestamp as i16 => Ok(ErrorCode::InvalidTimestamp),
            i if i == ErrorCode::UnsupportedSaslMechanism as i16 => Ok(ErrorCode::UnsupportedSaslMechanism),
            i if i == ErrorCode::IllegalSaslState as i16 => Ok(ErrorCode::IllegalSaslState),
            i if i == ErrorCode::UnsupportedVersion as i16 => Ok(ErrorCode::UnsupportedVersion),
            i if i == ErrorCode::TopicAlreadyExists as i16 => Ok(ErrorCode::TopicAlreadyExists),
            i if i == ErrorCode::InvalidPartitions as i16 => Ok(ErrorCode::InvalidPartitions),
            i if i == ErrorCode::InvalidReplicationFactor as i16 => Ok(ErrorCode::InvalidReplicationFactor),
            i if i == ErrorCode::InvalidReplicaAssignment as i16 => Ok(ErrorCode::InvalidReplicaAssignment),
            i if i == ErrorCode::InvalidConfig as i16 => Ok(ErrorCode::InvalidConfig),
            i if i == ErrorCode::NotController as i16 => Ok(ErrorCode::NotController),
            i if i == ErrorCode::InvalidRequest as i16 => Ok(ErrorCode::InvalidRequest),
            i if i == ErrorCode::UnsupportedForMessageFormat as i16 => Ok(ErrorCode::UnsupportedForMessageFormat),
            i if i == ErrorCode::PolicyViolation as i16 => Ok(ErrorCode::PolicyViolation),
            i if i == ErrorCode::OutOfOrderSequenceNumber as i16 => Ok(ErrorCode::OutOfOrderSequenceNumber),
            i if i == ErrorCode::DuplicateSequenceNumber as i16 => Ok(ErrorCode::DuplicateSequenceNumber),
            i if i == ErrorCode::InvalidProducerEpoch as i16 => Ok(ErrorCode::InvalidProducerEpoch),
            i if i == ErrorCode::InvalidTxnState as i16 => Ok(ErrorCode::InvalidTxnState),
            i if i == ErrorCode::InvalidProducerIdMapping as i16 => Ok(ErrorCode::InvalidProducerIdMapping),
            i if i == ErrorCode::InvalidTransactionTimeout as i16 => Ok(ErrorCode::InvalidTransactionTimeout),
            i if i == ErrorCode::ConcurrentTransactions as i16 => Ok(ErrorCode::ConcurrentTransactions),
            i if i == ErrorCode::TransactionCoordinatorFenced as i16 => Ok(ErrorCode::TransactionCoordinatorFenced),
            i if i == ErrorCode::TransactionalIdAuthorizationFailed as i16 => Ok(ErrorCode::TransactionalIdAuthorizationFailed),
            i if i == ErrorCode::SecurityDisabled as i16 => Ok(ErrorCode::SecurityDisabled),
            i if i == ErrorCode::OperationNotAttempted as i16 => Ok(ErrorCode::OperationNotAttempted),
            i if i == ErrorCode::KafkaStorageError as i16 => Ok(ErrorCode::KafkaStorageError),
            i if i == ErrorCode::LogDirNotFound as i16 => Ok(ErrorCode::LogDirNotFound),
            i if i == ErrorCode::SaslAuthenticationFailed as i16 => Ok(ErrorCode::SaslAuthenticationFailed),
            i if i == ErrorCode::UnknownProducerId as i16 => Ok(ErrorCode::UnknownProducerId),
            i if i == ErrorCode::ReassignmentInProgress as i16 => Ok(ErrorCode::ReassignmentInProgress),
            i if i == ErrorCode::DelegationTokenAuthDisabled as i16 => Ok(ErrorCode::DelegationTokenAuthDisabled),
            i if i == ErrorCode::DelegationTokenNotFound as i16 => Ok(ErrorCode::DelegationTokenNotFound),
            i if i == ErrorCode::DelegationTokenOwnerMismatch as i16 => Ok(ErrorCode::DelegationTokenOwnerMismatch),
            i if i == ErrorCode::DelegationTokenRequestNotAllowed as i16 => Ok(ErrorCode::DelegationTokenRequestNotAllowed),
            i if i == ErrorCode::DelegationTokenAuthorizationFailed as i16 => Ok(ErrorCode::DelegationTokenAuthorizationFailed),
            i if i == ErrorCode::DelegationTokenExpired as i16 => Ok(ErrorCode::DelegationTokenExpired),
            i if i == ErrorCode::InvalidPrincipalType as i16 => Ok(ErrorCode::InvalidPrincipalType),
            i if i == ErrorCode::NonEmptyGroup as i16 => Ok(ErrorCode::NonEmptyGroup),
            i if i == ErrorCode::GroupIdNotFound as i16 => Ok(ErrorCode::GroupIdNotFound),
            i if i == ErrorCode::FetchSessionIdNotFound as i16 => Ok(ErrorCode::FetchSessionIdNotFound),
            i if i == ErrorCode::InvalidFetchSessionEpoch as i16 => Ok(ErrorCode::InvalidFetchSessionEpoch),
            i if i == ErrorCode::ListenerNotFound as i16 => Ok(ErrorCode::ListenerNotFound),
            i if i == ErrorCode::TopicDeletionDisabled as i16 => Ok(ErrorCode::TopicDeletionDisabled),
            i if i == ErrorCode::FencedLeaderEpoch as i16 => Ok(ErrorCode::FencedLeaderEpoch),
            i if i == ErrorCode::UnknownLeaderEpoch as i16 => Ok(ErrorCode::UnknownLeaderEpoch),
            i if i == ErrorCode::UnsupportedCompressionType as i16 => Ok(ErrorCode::UnsupportedCompressionType),
            i if i == ErrorCode::StaleBrokerEpoch as i16 => Ok(ErrorCode::StaleBrokerEpoch),
            i if i == ErrorCode::OffsetNotAvailable as i16 => Ok(ErrorCode::OffsetNotAvailable),
            i if i == ErrorCode::MemberIdRequired as i16 => Ok(ErrorCode::MemberIdRequired),
            i if i == ErrorCode::PreferredLeaderNotAvailable as i16 => Ok(ErrorCode::PreferredLeaderNotAvailable),
            i if i == ErrorCode::GroupMaxSizeReached as i16 => Ok(ErrorCode::GroupMaxSizeReached),
            i if i == ErrorCode::FencedInstanceId as i16 => Ok(ErrorCode::FencedInstanceId),
            i if i == ErrorCode::EligibleLeadersNotAvailable as i16 => Ok(ErrorCode::EligibleLeadersNotAvailable),
            i if i == ErrorCode::ElectionNotNeeded as i16 => Ok(ErrorCode::ElectionNotNeeded),
            i if i == ErrorCode::NoReassignmentInProgress as i16 => Ok(ErrorCode::NoReassignmentInProgress),
            i if i == ErrorCode::GroupSubscribedToTopic as i16 => Ok(ErrorCode::GroupSubscribedToTopic),
            i if i == ErrorCode::InvalidRecord as i16 => Ok(ErrorCode::InvalidRecord),
            i if i == ErrorCode::UnstableOffsetCommit as i16 => Ok(ErrorCode::UnstableOffsetCommit),
            i if i == ErrorCode::ThrottlingQuotaExceeded as i16 => Ok(ErrorCode::ThrottlingQuotaExceeded),
            i if i == ErrorCode::ProducerFenced as i16 => Ok(ErrorCode::ProducerFenced),
            i if i == ErrorCode::ResourceNotFound as i16 => Ok(ErrorCode::ResourceNotFound),
            i if i == ErrorCode::DuplicateResource as i16 => Ok(ErrorCode::DuplicateResource),
            i if i == ErrorCode::UnacceptableCredential as i16 => Ok(ErrorCode::UnacceptableCredential),
            i if i == ErrorCode::InconsistentVoterSet as i16 => Ok(ErrorCode::InconsistentVoterSet),
            i if i == ErrorCode::InvalidUpdateVersion as i16 => Ok(ErrorCode::InvalidUpdateVersion),
            i if i == ErrorCode::FeatureUpdateFailed as i16 => Ok(ErrorCode::FeatureUpdateFailed),
            i if i == ErrorCode::PrincipalDeserializationFailure as i16 => Ok(ErrorCode::PrincipalDeserializationFailure),
            i if i == ErrorCode::SnapshotNotFound as i16 => Ok(ErrorCode::SnapshotNotFound),
            i if i == ErrorCode::PositionOutOfRange as i16 => Ok(ErrorCode::PositionOutOfRange),
            i if i == ErrorCode::UnknownTopicId as i16 => Ok(ErrorCode::UnknownTopicId),
            i if i == ErrorCode::DuplicateBrokerRegistration as i16 => Ok(ErrorCode::DuplicateBrokerRegistration),
            i if i == ErrorCode::BrokerIdNotRegistered as i16 => Ok(ErrorCode::BrokerIdNotRegistered),
            i if i == ErrorCode::InconsistentTopicId as i16 => Ok(ErrorCode::InconsistentTopicId),
            i if i == ErrorCode::InconsistentClusterId as i16 => Ok(ErrorCode::InconsistentClusterId),
            i if i == ErrorCode::TransactionalIdNotFound as i16 => Ok(ErrorCode::TransactionalIdNotFound),
            i if i == ErrorCode::FetchSessionTopicIdError as i16 => Ok(ErrorCode::FetchSessionTopicIdError),
            i if i == ErrorCode::IneligibleReplica as i16 => Ok(ErrorCode::IneligibleReplica),
            i if i == ErrorCode::NewLeaderElected as i16 => Ok(ErrorCode::NewLeaderElected),
            _ => Err(anyhow!("Unable to determine error code for value {}", value))
        }
    }
}

impl KafkaEncodable for ErrorCode {
    fn to_kafka_bytes<W: Write + Debug>(self, write_buffer: &mut W) -> Result<()> {
        (self as i16).to_kafka_bytes(write_buffer)
    }
}

impl KafkaDecodable for ErrorCode {
    fn from_kafka_bytes<R: Read + Debug>(read_buffer: &mut R) -> Result<Self> {
        let code: i16 = i16::from_kafka_bytes(read_buffer)?;
        ErrorCode::try_from(code)
    }
}
