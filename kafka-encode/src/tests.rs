use std::str::FromStr;
use tracing::Level;
use uuid::Uuid;
use ctor::ctor;
use crate::KafkaEncodable;
use crate::primitives::{NullableArray, CompactNullableArray, CompactBytes, CompactNullableBytes, CompactNullableString, CompactString, NullableBytes, NullableString, UnsignedVarInt32, VarI32, VarI64, Array, CompactArray};

// TODO: figure out how to make the writer to stdout not deadlock with multiple tests
// #[ctor]
fn print_spans_and_events_during_tests() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::TRACE)
        // .with_span_events(Span::FULL)
        .init();
}

macro_rules! test_serialize {
    ($object:expr, $expected_bytes:expr) => {
        let mut write_buffer: Vec<u8> = Vec::new();

        $object.to_kafka_bytes(&mut write_buffer).expect("Unable to serialize object");
        assert_eq!(write_buffer, $expected_bytes);
    }
}

macro_rules! test_deserialize {
    ($bytes:expr, $object_type:ty, $expected_object:expr) => {
        let object = <$object_type>::from_kafka_bytes(&mut &*$bytes).expect("Unable to deserialize object");
        assert_eq!(object, $expected_object);
    }
}

// BOOLEAN
#[test]
fn test_serialize_true() {
    test_serialize!(true, vec![1u8]);
}

#[test]
fn test_serialize_false() {
    test_serialize!(false, vec![0u8]);
}

#[test]
fn test_deserialize_true() {
    test_deserialize!(vec![1u8], bool, true);
}

#[test]
fn test_deserialize_false() {
    test_deserialize!(vec![0u8], bool, false);
}

// INT8

#[test]
fn test_serialize_i8() {
    test_serialize!(0i8, vec![0u8]);
    test_serialize!(1i8, vec![1u8]);
    test_serialize!(i8::MAX, vec![127u8]);
    test_serialize!(i8::MIN, vec![128u8]);
    test_serialize!(-1i8, vec![255u8]);
}

#[test]
fn test_deserialize_i8() {
    test_deserialize!(vec![0u8], i8, 0i8);
    test_deserialize!(vec![1u8], i8, 1i8);
    test_deserialize!(vec![127u8], i8, i8::MAX);
    test_deserialize!(vec![128u8], i8, i8::MIN);
    test_deserialize!(vec![255u8], i8, -1i8);
}

// INT16
#[test]
fn test_serialize_i16() {
    test_serialize!(0i16, vec![0u8, 0u8]);
    test_serialize!(1i16, vec![0u8, 1u8]);
    test_serialize!(i16::MAX, vec![127u8, 255u8]);
    test_serialize!(i16::MIN, vec![128u8, 0u8]);
    test_serialize!(-1i16, vec![255u8, 255u8]);
}

#[test]
fn test_deserialize_i16() {
    test_deserialize!(vec![0u8, 0u8], i16, 0i16);
    test_deserialize!(vec![0u8, 1u8], i16, 1i16);
    test_deserialize!(vec![127u8, 255u8], i16, i16::MAX);
    test_deserialize!(vec![128u8, 0u8], i16, i16::MIN);
    test_deserialize!(vec![255u8, 255u8], i16, -1i16);
}

// INT32
#[test]
fn test_serialize_i32() {
    test_serialize!(0i32, vec![0u8, 0u8, 0u8, 0u8]);
    test_serialize!(1i32, vec![0u8, 0u8, 0u8, 1u8]);
    test_serialize!(i32::MAX, vec![127u8, 255u8, 255u8, 255u8]);
    test_serialize!(i32::MIN, vec![128u8, 0u8, 0u8, 0u8]);
    test_serialize!(-1i32, vec![255u8, 255u8, 255u8, 255u8]);
}

#[test]
fn test_deserialize_i32() {
    test_deserialize!(vec![0u8, 0u8, 0u8, 0u8], i32, 0i32);
    test_deserialize!(vec![0u8, 0u8, 0u8, 1u8], i32, 1i32);
    test_deserialize!(vec![127u8, 255u8, 255u8, 255u8], i32, i32::MAX);
    test_deserialize!(vec![128u8, 0u8, 0u8, 0u8], i32, i32::MIN);
    test_deserialize!(vec![255u8, 255u8, 255u8, 255u8], i32, -1i32);
}

// INT64
#[test]
fn test_serialize_i64() {
    test_serialize!(0i64, vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]);
    test_serialize!(1i64, vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8]);
    test_serialize!(i64::MAX, vec![127u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8]);
    test_serialize!(i64::MIN, vec![128u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]);
    test_serialize!(-1i64, vec![255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8]);
}

#[test]
fn test_deserialize_i64() {
    test_deserialize!(vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8], i64, 0i64);
    test_deserialize!(vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8], i64, 1i64);
    test_deserialize!(vec![127u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8], i64, i64::MAX);
    test_deserialize!(vec![128u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8], i64, i64::MIN);
    test_deserialize!(vec![255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8, 255u8], i64, -1i64);
}

// UINT32
#[test]
fn test_serialize_u32() {
    test_serialize!(0u32, vec![0u8, 0u8, 0u8, 0u8]);
    test_serialize!(1u32, vec![0u8, 0u8, 0u8, 1u8]);
    test_serialize!(u32::MAX, vec![255u8, 255u8, 255u8, 255u8]);
}

#[test]
fn test_deserialize_u32() {
    test_deserialize!(vec![0u8, 0u8, 0u8, 0u8], u32, 0u32);
    test_deserialize!(vec![0u8, 0u8, 0u8, 1u8], u32, 1u32);
    test_deserialize!(vec![255u8, 255u8, 255u8, 255u8], u32, u32::MAX);
}

// VARINT
#[test]
fn test_serialize_var_i32() {
    test_serialize!(VarI32(0), vec![0u8]);
    test_serialize!(VarI32(-1), vec![1u8]);
    test_serialize!(VarI32(1), vec![2u8]);

}

#[test]
fn test_deserialize_var_i32() {
    test_deserialize!(vec![0u8], VarI32, VarI32(0));
    test_deserialize!(vec![1u8], VarI32, VarI32(-1));
    test_deserialize!(vec![2u8], VarI32, VarI32(1));
}

// VARLONG
#[test]
fn test_serialize_var_i64() {
    test_serialize!(VarI64(0), vec![0u8]);
    test_serialize!(VarI64(-1), vec![1u8]);
    test_serialize!(VarI64(1), vec![2u8]);

}

#[test]
fn test_deserialize_var_i64() {
    test_deserialize!(vec![0u8], VarI64, VarI64(0));
    test_deserialize!(vec![1u8], VarI64, VarI64(-1));
    test_deserialize!(vec![2u8], VarI64, VarI64(1));
}

// UUID
#[test]
fn test_serialize_uuid() {
    let uuid: Uuid = Uuid::from_str("67e55044-10b1-426f-9247-bb680e5fe0c8").expect("Unable to construct a UUID");
    test_serialize!(uuid, vec![103, 229, 80, 68, 16, 177, 66, 111, 146, 71, 187, 104, 14, 95, 224, 200]);
}

#[test]
fn test_deserialize_uuid() {
    let uuid: Uuid = Uuid::from_str("67e55044-10b1-426f-9247-bb680e5fe0c8").expect("Unable to construct a UUID");
    test_deserialize!(vec![103, 229, 80, 68, 16, 177, 66, 111, 146, 71, 187, 104, 14, 95, 224, 200], Uuid, uuid);
}

// TODO: FLOAT64 tests

// STRING
#[test]
fn test_serialize_string() {
    test_serialize!(String::from("lol"), vec![0, 3, 108, 111, 108]);
    test_serialize!(String::from(""), vec![0, 0]);

}

#[test]
fn test_deserialize_string() {
    test_deserialize!(vec![0, 3, 108, 111, 108], String, String::from("lol"));
    test_deserialize!(vec![0, 0], String, String::from(""));
}

// UNSIGNED_VARINT
#[test]
fn test_serialize_var_u32() {
    test_serialize!(UnsignedVarInt32(0), vec![0u8]);
    test_serialize!(UnsignedVarInt32(1), vec![1u8]);
    test_serialize!(UnsignedVarInt32(2), vec![2u8]);
}

#[test]
fn test_deserialize_var_u32() {
    test_deserialize!(vec![0u8], UnsignedVarInt32, UnsignedVarInt32(0));
    test_deserialize!(vec![1u8], UnsignedVarInt32, UnsignedVarInt32(1));
    test_deserialize!(vec![2u8], UnsignedVarInt32, UnsignedVarInt32(2));
}

// COMPACT_STRING
#[test]
fn test_serialize_compact_string() {
    test_serialize!(CompactString(String::from("lol")), vec![4, 108, 111, 108]);
    test_serialize!(CompactString(String::from("")), vec![1]);
}

#[test]
fn test_deserialize_compact_string() {
    test_deserialize!(vec![4, 108, 111, 108], CompactString, CompactString(String::from("lol")));
    test_deserialize!(vec![1], CompactString, CompactString(String::from("")));
}

// NULLABLE_STRING
#[test]
fn test_serialize_nullable_string() {
    test_serialize!(NullableString(Some(String::from("lol"))), vec![0, 3, 108, 111, 108]);
    test_serialize!(NullableString(Some(String::from(""))), vec![0, 0]);
    test_serialize!(NullableString(None), vec![255, 255]);
}

#[test]
fn test_deserialize_nullable_string() {
    test_deserialize!(vec![0, 3, 108, 111, 108], NullableString, NullableString(Some(String::from("lol"))));
    test_deserialize!(vec![0, 0], NullableString, NullableString(Some(String::from(""))));
    test_deserialize!(vec![255, 255], NullableString, NullableString(None));
}

// COMPACT_NULLABLE_STRING
#[test]
fn test_serialize_compact_nullable_string() {
    test_serialize!(CompactNullableString(Some(String::from("lol"))), vec![4, 108, 111, 108]);
    test_serialize!(CompactNullableString(Some(String::from(""))), vec![1]);
    test_serialize!(CompactNullableString(None), vec![0]);
}

#[test]
fn test_deserialize_compact_nullable_string() {
    test_deserialize!(vec![4, 108, 111, 108], CompactNullableString, CompactNullableString(Some(String::from("lol"))));
    test_deserialize!(vec![1], CompactNullableString, CompactNullableString(Some(String::from(""))));
    test_deserialize!(vec![0], CompactNullableString, CompactNullableString(None));
}

// BYTES
#[test]
fn test_serialize_bytes() {
    test_serialize!(vec![9, 8, 7, 6, 5], vec![0, 0, 0, 5, 9, 8, 7, 6, 5]);
    test_serialize!(vec![], vec![0, 0, 0, 0]);
}

#[test]
fn test_deserialize_bytes() {
    test_deserialize!(vec![0, 0, 0, 5, 9, 8, 7, 6, 5], Vec::<u8>, vec![9, 8, 7, 6, 5]);
    test_deserialize!(vec![0, 0, 0, 0], Vec::<u8>, vec![]);
}

// COMPACT BYTES
#[test]
fn test_serialize_compact_bytes() {
    test_serialize!(CompactBytes(vec![9, 8, 7, 6, 5]), vec![6, 9, 8, 7, 6, 5]);
    test_serialize!(CompactBytes(vec![]), vec![1]);
}

#[test]
fn test_deserialize_compact_bytes() {
    test_deserialize!(vec![6, 9, 8, 7, 6, 5], CompactBytes, CompactBytes(vec![9, 8, 7, 6, 5]));
    test_deserialize!(vec![1], CompactBytes, CompactBytes(vec![]));
}

// NULLABLE_BYTES
#[test]
fn test_serialize_nullable_bytes() {
    test_serialize!(NullableBytes(Some(vec![9, 8, 7, 6, 5])), vec![0, 0, 0, 5, 9, 8, 7, 6, 5]);
    test_serialize!(NullableBytes(Some(vec![])), vec![0, 0, 0, 0]);
    test_serialize!(NullableBytes(None), vec![255, 255, 255, 255]);
}

#[test]
fn test_deserialize_nullable_bytes() {
    test_deserialize!(vec![0, 0, 0, 5, 9, 8, 7, 6, 5], NullableBytes, NullableBytes(Some(vec![9, 8, 7, 6, 5])));
    test_deserialize!(vec![0, 0, 0, 0], NullableBytes, NullableBytes(Some(vec![])));
    test_deserialize!(vec![255, 255, 255, 255], NullableBytes, NullableBytes(None));
}

// COMPACT_NULLABLE_BYTES
#[test]
fn test_serialize_compact_nullable_bytes() {
    test_serialize!(CompactNullableBytes(Some(vec![9, 8, 7, 6, 5])), vec![6, 9, 8, 7, 6, 5]);
    test_serialize!(CompactNullableBytes(Some(vec![])), vec![1]);
    test_serialize!(CompactNullableBytes(None), vec![0]);
}

#[test]
fn test_deserialize_compact_nullable_bytes() {
    test_deserialize!(vec![6, 9, 8, 7, 6, 5], CompactNullableBytes, CompactNullableBytes(Some(vec![9, 8, 7, 6, 5])));
    test_deserialize!(vec![1], CompactNullableBytes, CompactNullableBytes(Some(vec![])));
    test_deserialize!(vec![0], CompactNullableBytes, CompactNullableBytes(None));
}

// ARRAY
#[test]
fn test_serialize_array() {
    test_serialize!(Array::<VarI32>::new(vec![VarI32(1), VarI32(42)]), vec![0, 0, 0, 2, 2, 84]);
    test_serialize!(Array::<VarI32>::new(vec![]), vec![0, 0, 0, 0]);
}

#[test]
fn test_deserialize_array() {
    test_deserialize!(vec![0, 0, 0, 2, 2, 84], Array::<VarI32>, Array::<VarI32>::new(vec![VarI32(1), VarI32(42)]));
    test_deserialize!(vec![0, 0, 0, 0], Array::<VarI32>, Array::<VarI32>::new(vec![]));
}

// NULLABLE_ARRAY
#[test]
fn test_serialize_nullable_array() {
    test_serialize!(NullableArray::<VarI32>::new(Some(vec![VarI32(1), VarI32(42)])), vec![0, 0, 0, 2, 2, 84]);
    test_serialize!(NullableArray::<VarI32>::new(None), vec![255, 255, 255, 255]);
}

#[test]
fn test_deserialize_nullable_array() {
    test_deserialize!(vec![0, 0, 0, 2, 2, 84], NullableArray::<VarI32>, NullableArray::<VarI32>::new(Some(vec![VarI32(1), VarI32(42)])));
    test_deserialize!(vec![255, 255, 255, 255], NullableArray::<VarI32>, NullableArray::<VarI32>::new(None));
}

// COMPACT_ARRAY
#[test]
fn test_serialize_compact_array() {
    test_serialize!(CompactArray::<VarI32>::new(vec![VarI32(1), VarI32(42)]), vec![3, 2, 84]);
    test_serialize!(CompactArray::<VarI32>::new(vec![]), vec![1]);
}

#[test]
fn test_deserialize_compact_array() {
    test_deserialize!(vec![3, 2, 84], CompactArray::<VarI32>, CompactArray::<VarI32>::new(vec![VarI32(1), VarI32(42)]));
    test_deserialize!(vec![1], CompactArray::<VarI32>, CompactArray::<VarI32>::new(vec![]));
}

// COMPACT_NULLABLE_ARRAY
#[test]
fn test_serialize_compact_nullable_array() {
    test_serialize!(CompactNullableArray::<VarI32>::new(Some(vec![VarI32(1), VarI32(42)])), vec![3, 2, 84]);
    test_serialize!(CompactNullableArray::<VarI32>::new(None), vec![0]);
}

#[test]
fn test_deserialize_compact_nullable_array() {
    test_deserialize!(vec![3, 2, 84], CompactNullableArray::<VarI32>, CompactNullableArray::<VarI32>::new(Some(vec![VarI32(1), VarI32(42)])));
    test_deserialize!(vec![0], CompactNullableArray::<VarI32>, CompactNullableArray::<VarI32>::new(None));
}
