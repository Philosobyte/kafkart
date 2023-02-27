use std::str::FromStr;
use tracing::Level;
use uuid::Uuid;
use ctor::ctor;
use crate::{KafkaDecodable, KafkaEncodable};
use crate::primitives::{NullableArray, CompactNullableArray, CompactBytes, CompactNullableBytes, CompactNullableString, CompactString, NullableBytes, NullableString, UnsignedVarInt32, VarI32, VarI64, Array, CompactArray};

#[ctor]
fn print_spans_and_events_during_tests() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        // TODO: figure out how to make the writer to stdout not deadlock with multiple tests
        // .with_span_events(FmtSpan::FULL)
        .init();
}

// BOOLEAN
#[test]
fn test_serialize_true() {
    let mut write_buffer: Vec<u8> = Vec::new();

    true.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![1u8]);
}

#[test]
fn test_serialize_false() {
    let mut write_buffer: Vec<u8> = Vec::new();

    false.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0u8]);
}

#[test]
fn test_deserialize_true() {
    let read_buffer: Vec<u8> = vec![1u8];
    let b: bool = bool::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(b, true)
}

#[test]
fn test_deserialize_false() {
    let read_buffer: Vec<u8> = vec![0u8];
    let b: bool = bool::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(b, false)
}

// INT8

#[test]
fn test_serialize_i8() {
    let mut write_buffer: Vec<u8> = Vec::new();
    0i8.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0u8]);
    write_buffer.clear();

    1i8.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![1u8]);
    write_buffer.clear();

    i8::MAX.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![127u8]);
    write_buffer.clear();

    i8::MIN.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![128u8]);
    write_buffer.clear();

    (-1i8).to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![255u8]);
    write_buffer.clear();
}

#[test]
fn test_deserialize_i8() {
    let mut read_buffer: Vec<u8> = vec![0u8];
    let i: i8 = i8::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 0i8);

    read_buffer = vec![1u8];
    let i: i8 = i8::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 1i8);

    read_buffer = vec![127u8];
    let i: i8 = i8::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, i8::MAX);

    read_buffer = vec![128u8];
    let i: i8 = i8::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, i8::MIN);

    read_buffer = vec![255u8];
    let i: i8 = i8::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, -1i8);
}

// INT16
#[test]
fn test_serialize_i16() {
    let mut write_buffer: Vec<u8> = Vec::new();
    0i16.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0u8, 0u8]);
    write_buffer.clear();

    1i16.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0u8, 1u8]);
    write_buffer.clear();

    i16::MAX.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![127u8, 255u8]);
    write_buffer.clear();

    i16::MIN.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![128u8, 0u8]);
    write_buffer.clear();

    (-1i16).to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![255u8, 255u8]);
    write_buffer.clear();
}

#[test]
fn test_deserialize_i16() {
    let mut read_buffer: Vec<u8> = vec![0u8, 0u8];
    let i: i16 = i16::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 0i16);

    read_buffer = vec![0u8, 1u8];
    let i: i16 = i16::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 1i16);

    read_buffer = vec![127u8, 255u8];
    let i: i16 = i16::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, i16::MAX);

    read_buffer = vec![128u8, 0u8];
    let i: i16 = i16::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, i16::MIN);

    read_buffer = vec![255u8, 255u8];
    let i: i16 = i16::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, -1i16);
}

// INT32
#[test]
fn test_serialize_i32() {
    let mut write_buffer: Vec<u8> = Vec::new();
    0i32.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0u8, 0u8, 0u8, 0u8]);
    write_buffer.clear();

    1i32.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0u8, 0u8, 0u8, 1u8]);
    write_buffer.clear();

    i32::MAX.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![127u8, 255u8, 255u8, 255u8]);
    write_buffer.clear();

    i32::MIN.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![128u8, 0u8, 0u8, 0u8]);
    write_buffer.clear();

    (-1i32).to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![255u8, 255u8, 255u8, 255u8]);
    write_buffer.clear();
}

#[test]
fn test_deserialize_i32() {
    let mut read_buffer: Vec<u8> = vec![0u8, 0u8, 0u8, 0u8];
    let i: i32 = i32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 0i32);

    read_buffer = vec![0u8, 0u8, 0u8, 1u8];
    let i: i32 = i32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 1i32);

    read_buffer = vec![127u8, 255u8, 255u8, 255u8];
    let i: i32 = i32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, i32::MAX);

    read_buffer = vec![128u8, 0u8, 0u8, 0u8];
    let i: i32 = i32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, i32::MIN);

    read_buffer = vec![255u8, 255u8, 255u8, 255u8];
    let i: i32 = i32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, -1i32);
}

// INT64
#[test]
fn test_serialize_i64() {
    let mut write_buffer: Vec<u8> = Vec::new();
    0i64.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]);
    write_buffer.clear();

    1i64.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8]);
    write_buffer.clear();

    i64::MAX.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![127u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8]);
    write_buffer.clear();

    i64::MIN.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![128u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]);
    write_buffer.clear();

    (-1i64).to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8]);
    write_buffer.clear();

}

#[test]
fn test_deserialize_i64() {
    let mut read_buffer: Vec<u8> = vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    let i: i64 = i64::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 0i64);

    read_buffer = vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8];
    let i: i64 = i64::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 1i64);

    read_buffer = vec![127u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8];
    let i: i64 = i64::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, i64::MAX);

    read_buffer = vec![128u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    let i: i64 = i64::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, i64::MIN);

    read_buffer = vec![255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8];
    let i: i64 = i64::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, -1i64);
}

// UINT32
#[test]
fn test_serialize_u32() {
    let mut buffer: Vec<u8> = Vec::new();
    0u32.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0u8, 0u8, 0u8, 0u8]);
    buffer.clear();

    1u32.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0u8, 0u8, 0u8, 1u8]);
    buffer.clear();

    u32::MAX.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![255u8, 255u8, 255u8, 255u8]);
    buffer.clear();
}

#[test]
fn test_deserialize_u32() {
    let mut read_buffer: Vec<u8> = vec![0u8, 0u8, 0u8, 0u8];
    let i: u32 = u32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 0u32);

    read_buffer = vec![0u8, 0u8, 0u8, 1u8];
    let i: u32 = u32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, 1u32);

    read_buffer = vec![255u8, 255u8, 255u8, 255u8];
    let i: u32 = u32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(i, u32::MAX);
}

// VARINT
#[test]
fn test_serialize_var_i32() {
    let mut buffer: Vec<u8> = Vec::new();
    let zero: VarI32 = VarI32(0);
    zero.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0u8]);
    buffer.clear();

    let negative_one: VarI32 = VarI32(-1);
    negative_one.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![1u8]);
    buffer.clear();

    let one: VarI32 = VarI32(1);
    one.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![2u8]);
    buffer.clear();

}

#[test]
fn test_deserialize_var_i32() {
    let mut read_buffer: Vec<u8> = vec![0u8];
    let zero: VarI32 = VarI32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(zero, VarI32(0));

    read_buffer = vec![1u8];
    let negative_one: VarI32 = VarI32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(negative_one, VarI32(-1));

    read_buffer = vec![2u8];
    let one: VarI32 = VarI32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(one, VarI32(1));
}

// VARLONG
#[test]
fn test_serialize_var_i64() {
    let mut buffer: Vec<u8> = Vec::new();
    let zero: VarI64 = VarI64(0);
    zero.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0u8]);
    buffer.clear();

    let negative_one: VarI64 = VarI64(-1);
    negative_one.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![1u8]);
    buffer.clear();

    let one: VarI64 = VarI64(1);
    one.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![2u8]);
    buffer.clear();

}

#[test]
fn test_deserialize_var_i64() {
    let mut read_buffer: Vec<u8> = vec![0u8];
    let zero: VarI64 = VarI64::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(zero, VarI64(0));

    read_buffer = vec![1u8];
    let negative_one: VarI64 = VarI64::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(negative_one, VarI64(-1));

    read_buffer = vec![2u8];
    let one: VarI64 = VarI64::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(one, VarI64(1));
}

// UUID
#[test]
fn test_serialize_uuid() {
    let mut write_buffer: Vec<u8> = Vec::new();
    let uuid: Uuid = Uuid::from_str("7a9ddb03821c4c7da1bc8db2c0120e85").unwrap();
    uuid.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![122, 157, 219, 3, 130, 28, 76, 125, 161, 188, 141, 178, 192, 18, 14, 133]);
}

#[test]
fn test_deserialize_uuid() {
    let read_buffer: Vec<u8> = vec![122, 157, 219, 3, 130, 28, 76, 125, 161, 188, 141, 178, 192, 18, 14, 133];
    let uuid: Uuid = Uuid::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(uuid, Uuid::from_str("7a9ddb03821c4c7da1bc8db2c0120e85").unwrap());
}

// TODO: FLOAT64 tests

// STRING
#[test]
fn test_serialize_string() {
    let mut buffer: Vec<u8> = Vec::new();
    let s: String = String::from("lol");
    s.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0, 3, 108, 111, 108]);
    buffer.clear();

    let s: String = String::from("");
    s.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0, 0]);
    buffer.clear();

}

#[test]
fn test_deserialize_string() {
    let mut read_buffer: Vec<u8> = vec![0, 3, 108, 111, 108];
    let s: String = String::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(s, String::from("lol"));

    read_buffer = vec![0, 0];
    let s: String = String::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(s, String::from(""));
}

// UNSIGNED_VARINT
#[test]
fn test_serialize_var_u32() {
    let mut buffer: Vec<u8> = Vec::new();
    let zero: UnsignedVarInt32 = UnsignedVarInt32(0);
    zero.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0u8]);
    buffer.clear();

    let negative_one: UnsignedVarInt32 = UnsignedVarInt32(1);
    negative_one.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![1u8]);
    buffer.clear();

    let one: UnsignedVarInt32 = UnsignedVarInt32(2);
    one.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![2u8]);
    buffer.clear();
}

#[test]
fn test_deserialize_var_u32() {
    let mut read_buffer: Vec<u8> = vec![0u8];
    let zero: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(zero, UnsignedVarInt32(0));

    read_buffer = vec![1u8];
    let negative_one: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(negative_one, UnsignedVarInt32(1));

    read_buffer = vec![2u8];
    let one: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(one, UnsignedVarInt32(2));
}

// COMPACT_STRING
#[test]
fn test_serialize_compact_string() {
    let mut buffer: Vec<u8> = Vec::new();
    let compact_string: CompactString = CompactString(String::from("lol"));
    compact_string.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![4, 108, 111, 108]);
    buffer.clear();

    let compact_string = CompactString(String::from(""));
    compact_string.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![1]);
    buffer.clear();
}

#[test]
fn test_deserialize_compact_string() {
    let mut read_buffer: Vec<u8> = vec![4, 108, 111, 108];
    let compact_string: CompactString = CompactString::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(compact_string, CompactString(String::from("lol")));

    read_buffer = vec![1];
    let compact_string: CompactString = CompactString::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(compact_string, CompactString(String::from("")));
}

// NULLABLE_STRING
#[test]
fn test_serialize_nullable_string() {
    let mut buffer: Vec<u8> = Vec::new();
    let nullable_string: NullableString = NullableString(Some(String::from("lol")));
    nullable_string.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0, 3, 108, 111, 108]);
    buffer.clear();

    let nullable_string: NullableString = NullableString(Some(String::from("")));
    nullable_string.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0, 0]);
    buffer.clear();

    let nullable_string: NullableString = NullableString(None);
    nullable_string.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![255, 255]);
    buffer.clear();
}

#[test]
fn test_deserialize_nullable_string() {
    let mut read_buffer: Vec<u8> = vec![0, 3, 108, 111, 108];
    let nullable_string: NullableString = NullableString::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(nullable_string, NullableString(Some(String::from("lol"))));

    read_buffer = vec![0, 0];
    let nullable_string: NullableString = NullableString::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(nullable_string, NullableString(Some(String::from(""))));

    read_buffer = vec![255, 255];
    let nullable_string: NullableString = NullableString::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(nullable_string, NullableString(None));
}

// COMPACT_NULLABLE_STRING
#[test]
fn test_serialize_compact_nullable_string() {
    let mut buffer: Vec<u8> = Vec::new();
    let string: CompactNullableString = CompactNullableString(Some(String::from("lol")));
    string.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![4, 108, 111, 108]);
    buffer.clear();

    let string: CompactNullableString = CompactNullableString(Some(String::from("")));
    string.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![1]);
    buffer.clear();

    let string: CompactNullableString = CompactNullableString(None);
    string.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0]);
    buffer.clear();
}

#[test]
fn test_deserialize_compact_nullable_string() {
    let mut read_buffer: Vec<u8> = vec![4, 108, 111, 108];
    let nullable_string: CompactNullableString = CompactNullableString::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(nullable_string, CompactNullableString(Some(String::from("lol"))));

    read_buffer = vec![1];
    let nullable_string: CompactNullableString = CompactNullableString::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(nullable_string, CompactNullableString(Some(String::from(""))));

    read_buffer = vec![0];
    let nullable_string: CompactNullableString = CompactNullableString::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(nullable_string, CompactNullableString(None));
}

// BYTES
#[test]
fn test_serialize_bytes() {
    let mut buffer: Vec<u8> = Vec::new();
    let bytes_to_serialize: Vec<u8> = vec![9, 8, 7, 6, 5];
    bytes_to_serialize.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0, 0, 0, 5, 9, 8, 7, 6, 5]);
    buffer.clear();

    let bytes_to_serialize: Vec<u8> = vec![];
    bytes_to_serialize.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0, 0, 0, 0]);
    buffer.clear();
}

#[test]
fn test_deserialize_bytes() {
    let mut read_buffer: Vec<u8> = vec![0, 0, 0, 5, 9, 8, 7, 6, 5];
    let deserialized_bytes: Vec<u8> = Vec::<u8>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, vec![9, 8, 7, 6, 5]);

    read_buffer = vec![0, 0, 0, 0];
    let deserialized_bytes: Vec<u8> = Vec::<u8>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, vec![]);
}

// COMPACT BYTES
#[test]
fn test_serialize_compact_bytes() {
    let mut buffer: Vec<u8> = Vec::new();
    let bytes_to_serialize: CompactBytes = CompactBytes(vec![9, 8, 7, 6, 5]);
    bytes_to_serialize.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![6, 9, 8, 7, 6, 5]);
    buffer.clear();

    let bytes_to_serialize: CompactBytes = CompactBytes(vec![]);
    bytes_to_serialize.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![1]);
    buffer.clear();
}

#[test]
fn test_deserialize_compact_bytes() {
    let mut read_buffer: Vec<u8> = vec![6, 9, 8, 7, 6, 5];
    let deserialized_bytes: CompactBytes = CompactBytes::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, CompactBytes(vec![9, 8, 7, 6, 5]));

    read_buffer = vec![1];
    let deserialized_bytes: CompactBytes = CompactBytes::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, CompactBytes(vec![]));
}

// NULLABLE_BYTES
#[test]
fn test_serialize_nullable_bytes() {
    let mut buffer: Vec<u8> = Vec::new();
    let bytes_to_serialize: NullableBytes = NullableBytes(Some(vec![9, 8, 7, 6, 5]));
    bytes_to_serialize.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0, 0, 0, 5, 9, 8, 7, 6, 5]);
    buffer.clear();

    let bytes_to_serialize: NullableBytes = NullableBytes(Some(vec![]));
    bytes_to_serialize.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![0, 0, 0, 0]);
    buffer.clear();

    let bytes_to_serialize: NullableBytes = NullableBytes(None);
    bytes_to_serialize.to_kafka_bytes(&mut buffer).unwrap();
    assert_eq!(buffer, vec![255, 255, 255, 255]);
    buffer.clear();

}

#[test]
fn test_deserialize_nullable_bytes() {
    let mut read_buffer: Vec<u8> = vec![0, 0, 0, 5, 9, 8, 7, 6, 5];
    let deserialized_bytes: NullableBytes = NullableBytes::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, NullableBytes(Some(vec![9, 8, 7, 6, 5])));

    read_buffer = vec![0, 0, 0, 0];
    let deserialized_bytes: NullableBytes = NullableBytes::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, NullableBytes(Some(vec![])));

    read_buffer = vec![255, 255, 255, 255];
    let deserialized_bytes: NullableBytes = NullableBytes::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, NullableBytes(None));
}

// COMPACT_NULLABLE_BYTES
#[test]
fn test_serialize_compact_nullable_bytes() {
    let mut write_buffer: Vec<u8> = Vec::new();
    let bytes_to_serialize: CompactNullableBytes = CompactNullableBytes(Some(vec![9, 8, 7, 6, 5]));
    bytes_to_serialize.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![6, 9, 8, 7, 6, 5]);
    write_buffer.clear();

    let bytes_to_serialize: CompactNullableBytes = CompactNullableBytes(Some(vec![]));
    bytes_to_serialize.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![1]);
    write_buffer.clear();

    let bytes_to_serialize: CompactNullableBytes = CompactNullableBytes(None);
    bytes_to_serialize.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0]);
    write_buffer.clear();
}

#[test]
fn test_deserialize_compact_nullable_bytes() {
    let mut read_buffer: Vec<u8> = vec![6, 9, 8, 7, 6, 5];
    let deserialized_bytes: CompactNullableBytes = CompactNullableBytes::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, CompactNullableBytes(Some(vec![9, 8, 7, 6, 5])));

    read_buffer = vec![1];
    let deserialized_bytes: CompactNullableBytes = CompactNullableBytes::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, CompactNullableBytes(Some(vec![])));

    read_buffer = vec![0];
    let deserialized_bytes: CompactNullableBytes = CompactNullableBytes::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(deserialized_bytes, CompactNullableBytes(None));
}

// ARRAY
#[test]
fn test_serialize_array() {
    let mut write_buffer: Vec<u8> = Vec::new();
    let array: Array<VarI32> = Array::<VarI32>::new(vec![VarI32(1), VarI32(42)]);

    array.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0, 0, 0, 2, 2, 84]);
    write_buffer.clear();

    let array: Array<VarI32> = Array::<VarI32>::new(Vec::new());

    array.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0, 0, 0, 0]);
    write_buffer.clear();
}

#[test]
fn test_deserialize_array() {
    let mut read_buffer: Vec<u8> = vec![0, 0, 0, 2, 2, 84];
    let array: Array<VarI32> = Array::<VarI32>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(array, Array::<VarI32>::new(vec![VarI32(1), VarI32(42)]));

    read_buffer = vec![0, 0, 0, 0];
    let array: Array<VarI32> = Array::<VarI32>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(array, Array::<VarI32>::new(Vec::new()));
}

// NULLABLE_ARRAY
#[test]
fn test_serialize_nullable_array() {
    let mut write_buffer: Vec<u8> = Vec::new();
    let nullable_array: NullableArray<VarI32> = NullableArray::<VarI32>::new(Some(vec![VarI32(1), VarI32(42)]));

    nullable_array.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0, 0, 0, 2, 2, 84]);
    write_buffer.clear();

    let nullable_array: NullableArray<VarI32> = NullableArray::<VarI32>::new(None);

    nullable_array.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![255, 255, 255, 255]);
    write_buffer.clear();
}

#[test]
fn test_deserialize_nullable_array() {
    let mut read_buffer: Vec<u8> = vec![0, 0, 0, 2, 2, 84];
    let nullable_array: NullableArray<VarI32> = NullableArray::<VarI32>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(nullable_array, NullableArray::<VarI32>::new(Some(vec![VarI32(1), VarI32(42)])));

    read_buffer = vec![255, 255, 255, 255];
    let nullable_array: NullableArray<VarI32> = NullableArray::<VarI32>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(nullable_array, NullableArray::<VarI32>::new(None));
}

// COMPACT_ARRAY
#[test]
fn test_serialize_compact_array() {
    let mut write_buffer: Vec<u8> = Vec::new();
    let compact_array: CompactArray<VarI32> = CompactArray::<VarI32>::new(vec![VarI32(1), VarI32(42)]);

    compact_array.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![3, 2, 84]);
    write_buffer.clear();

    let compact_array: CompactArray<VarI32> = CompactArray::<VarI32>::new(Vec::new());

    compact_array.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![1]);
    write_buffer.clear();
}

#[test]
fn test_deserialize_compact_array() {
    let mut read_buffer: Vec<u8> = vec![3, 2, 84];
    let compact_array: CompactArray<VarI32> = CompactArray::<VarI32>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(compact_array, CompactArray::<VarI32>::new(vec![VarI32(1), VarI32(42)]));

    read_buffer = vec![1];
    let compact_array: CompactArray<VarI32> = CompactArray::<VarI32>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(compact_array, CompactArray::<VarI32>::new(Vec::new()));
}

// COMPACT_NULLABLE_ARRAY
#[test]
fn test_serialize_compact_nullable_array() {
    let mut write_buffer: Vec<u8> = Vec::new();
    let compact_nullable_array: CompactNullableArray<VarI32> = CompactNullableArray::<VarI32>::new(Some(vec![VarI32(1), VarI32(42)]));

    compact_nullable_array.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![3, 2, 84]);
    write_buffer.clear();

    let compact_nullable_array: CompactNullableArray<VarI32> = CompactNullableArray::<VarI32>::new(None);

    compact_nullable_array.to_kafka_bytes(&mut write_buffer).unwrap();
    assert_eq!(write_buffer, vec![0]);
    write_buffer.clear();
}

#[test]
fn test_deserialize_compact_nullable_array() {
    let mut read_buffer: Vec<u8> = vec![3, 2, 84];
    let compact_nullable_array: CompactNullableArray<VarI32> = CompactNullableArray::<VarI32>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(compact_nullable_array, CompactNullableArray::<VarI32>::new(Some(vec![VarI32(1), VarI32(42)])));

    read_buffer = vec![0];
    let compact_nullable_array: CompactNullableArray<VarI32> = CompactNullableArray::<VarI32>::from_kafka_bytes(&mut &*read_buffer).unwrap();
    assert_eq!(compact_nullable_array, CompactNullableArray::<VarI32>::new(None));
}
