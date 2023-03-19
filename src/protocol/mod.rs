use std::collections::HashMap;
use std::fmt::Debug;
use std::intrinsics::write_bytes;
use std::io::{Error, ErrorKind};
use std::iter::Iterator;
use std::net::TcpStream;
use bytes::{BufMut, Bytes, BytesMut};
use crate::protocol::err::ErrorCode;
use kafka_encode_derive::KafkaEncodable;
use kafka_encode::KafkaEncodable;
use kafka_encode::primitives::*;
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
mod networking;

pub(crate) type ApiVersion = i16;

pub(crate) trait KafkaRequest: KafkaEncodable + Debug + PartialEq {
    fn get_api_key() -> ApiKey;
    fn get_version() -> ApiVersion;
    fn get_response_header_version() -> ApiVersion;
}

pub(crate) trait KafkaResponse: KafkaEncodable + Debug + PartialEq {
}

struct SupportedApiVersions {

}