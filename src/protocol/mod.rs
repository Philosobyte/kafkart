use std::collections::HashMap;
use std::intrinsics::write_bytes;
use std::io::{Error, ErrorKind};
use std::iter::Iterator;
use std::net::TcpStream;
use bytes::{BufMut, Bytes, BytesMut};
use crate::protocol::err::ErrorCode;
use kafka_encode_derive::{KafkaDecodable, KafkaEncodable};
use kafka_encode::{KafkaEncodable, KafkaDecodable};
use kafka_encode::primitives::{NullableArray, CompactNullableArray, CompactBytes, CompactNullableString, CompactString, NullableString, UnsignedVarInt32, VarI32, VarI64, Array, CompactArray, CompactNullableBytes, VarArray};
use crate::protocol::api_key::ApiKey;

mod err;
#[cfg(test)]
mod tests;
mod api_key;
mod headers;
mod records;
mod tags;
mod produce;
mod api_versions;
mod requests;

pub type ApiVersion = i16;
