use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use crate::{KafkaDecodable, KafkaEncodable};

macro_rules! impl_deref_for_single_field_tuple_struct {
    ($struct_name:ident, $target_type:ty) => {
        impl Deref for $struct_name {
            type Target = $target_type;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    }
}

// VARINT
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VarI32(pub i32);
impl_deref_for_single_field_tuple_struct!(VarI32, i32);

// VARLONG
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VarI64(pub i64);
impl_deref_for_single_field_tuple_struct!(VarI64, i64);

// UNSIGNED_VARINT
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnsignedVarInt32(pub u32);
impl_deref_for_single_field_tuple_struct!(UnsignedVarInt32, u32);

// COMPACT_STRING
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactString(pub String);
impl_deref_for_single_field_tuple_struct!(CompactString, String);

// NULLABLE_STRING
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NullableString(pub Option<String>);
impl_deref_for_single_field_tuple_struct!(NullableString, Option<String>);

// COMPACT_NULLABLE_STRING
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactNullableString(pub Option<String>);

impl_deref_for_single_field_tuple_struct!(CompactNullableString, Option<String>);

impl DerefMut for CompactNullableString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// COMPACT_BYTES
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactBytes(pub Vec<u8>);
impl_deref_for_single_field_tuple_struct!(CompactBytes, Vec<u8>);

// NULLABLE_BYTES
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NullableBytes(pub Option<Vec<u8>>);
impl_deref_for_single_field_tuple_struct!(NullableBytes, Option<Vec<u8>>);

// COMPACT_NULLABLE_BYTES
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactNullableBytes(pub Option<Vec<u8>>);

impl_deref_for_single_field_tuple_struct!(CompactNullableBytes, Option<Vec<u8>>);

impl DerefMut for CompactNullableBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// ARRAY
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Array<T: KafkaEncodable + KafkaDecodable + Debug>(pub Vec<T>);

impl<T: KafkaEncodable + KafkaDecodable + Debug> Array<T> {
    pub(crate) fn new(v: Vec<T>) -> Self {
        Array(v)
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> Deref for Array<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// NULLABLE_ARRAY
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NullableArray<T: KafkaEncodable + KafkaDecodable + Debug>(pub Option<Vec<T>>);

impl<T: KafkaEncodable + KafkaDecodable + Debug> NullableArray<T> {
    pub(crate) fn new(v: Option<Vec<T>>) -> Self {
        NullableArray(v)
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> Deref for NullableArray<T> {
    type Target = Option<Vec<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// COMPACT_ARRAY
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactArray<T: KafkaEncodable + KafkaDecodable + Debug>(pub Vec<T>);

impl<T: KafkaEncodable + KafkaDecodable + Debug> CompactArray<T> {
    pub(crate) fn new(v: Vec<T>) -> Self {
        CompactArray(v)
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> Deref for CompactArray<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// VARARRAY
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VarArray<T: KafkaEncodable + KafkaDecodable + Debug>(pub Vec<T>);

impl<T: KafkaEncodable + KafkaDecodable + Debug> VarArray<T> {
    pub(crate) fn new(v: Vec<T>) -> Self {
        VarArray(v)
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> Deref for VarArray<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// COMPACT_NULLABLE_ARRAY
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactNullableArray<T: KafkaEncodable + KafkaDecodable + Debug>(pub Option<Vec<T>>);

impl<T: KafkaEncodable + KafkaDecodable + Debug> CompactNullableArray<T> {
    pub(crate) fn new(v: Option<Vec<T>>) -> Self {
        CompactNullableArray(v)
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> Deref for CompactNullableArray<T> {
    type Target = Option<Vec<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
