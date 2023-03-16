use kafka_encode::primitives::{Array, CompactNullableBytes, CompactString, NullableArray, VarI32, VarI64};
use kafka_encode::{KafkaDecodable, KafkaEncodable};
use kafka_encode_derive::KafkaEncodable;

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct RecordHeader {
    pub key: CompactString,
    pub value: CompactNullableBytes
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct Record {
    pub length: VarI32,
    pub attributes: u8,
    pub timestamp_delta: VarI64,
    pub offset_delta: VarI32,
    pub key: CompactNullableBytes,
    pub value: CompactNullableBytes,
    pub headers: NullableArray<RecordHeader>
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch_offset: i16,
    pub base_sequence: i32,
    pub records: Array<Record>
}
