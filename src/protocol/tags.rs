use kafka_encode::primitives::{UnsignedVarInt32, VarArray};
use kafka_encode::KafkaEncodable;
use kafka_encode_derive::KafkaEncodable;

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct RawTaggedField {
    pub tag: UnsignedVarInt32,
    pub data: VarArray<u8>
}

#[derive(Debug, KafkaEncodable, Eq, PartialEq, Clone)]
pub struct TaggedFields {
    pub tags: VarArray<RawTaggedField>
}

impl TaggedFields {
    pub fn new() -> Self {
        TaggedFields {
            tags: VarArray(Vec::new())
        }
    }
}
