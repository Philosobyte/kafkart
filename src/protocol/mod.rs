use std::collections::HashMap;
use std::fmt::Debug;
use std::intrinsics::write_bytes;
use std::io::{Error, ErrorKind};
use std::iter::Iterator;
use std::net::TcpStream;
use bytes::{BufMut, Bytes, BytesMut};
use crate::protocol::err::ErrorCode;
use kafka_encode_derive::{KafkaDecodable, KafkaEncodable};
use kafka_encode::{KafkaEncodable, KafkaDecodable};
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

pub type ApiVersion = i16;

pub trait KafkaRequest: KafkaDecodable + KafkaEncodable + Debug {
    fn get_api_key() -> ApiKey;
    fn get_version() -> ApiVersion;
    fn get_response_header_version() -> ApiVersion;
}

pub trait KafkaResponse: KafkaDecodable + KafkaEncodable + Debug {
}