mod err;
pub mod primitives;
#[cfg(test)]
mod tests;
mod implementation;

use std::io::{Read, Result, Seek, Write};
use bytes::{Bytes, BytesMut};

// traits
/// Any type which implements this trait can be serialized in a way which follows Kafka's [networking protocol](https://kafka.apache.org/protocol.html).
///
/// # Examples
/// Here is what an implementation for `i32` could look like:
/// ```
/// impl KafkaEncodable for i32 {
///     fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
///         writer.write_all(&self.to_be_bytes())
///     }
/// }
/// ```
///
/// Here is how the implementation could be used:
/// ```
/// let mut buffer: BytesMut = BytesMut::new();
/// 0i32.to_kafka_bytes(&mut buffer).unwrap();
/// ```
pub trait KafkaEncodable {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()>;
}

/// Any type which implements this trait can be deserialized from a format which follows Kafka's [networking protocol](https://kafka.apache.org/protocol.html).
///
/// # Examples
/// Here is what an implementation for `i32` could look like:
/// ```
/// impl KafkaDecodable for i32 {
///     fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<i32> {
///         let mut buf: [u8; 4] = [0; 4];
///         reader.read_exact(&mut buf)?;
///         let i: i32 = i32::from_be_bytes(buf);
///
///         Ok(i)
///     }
/// }
/// ```
///
/// Here is how the implementation could be used:
/// ```
/// let mut buffer: Bytes = Bytes::from(vec![0u8, 0u8, 0u8, 0u8]);
/// let i: i32 = i32::from_kafka_bytes(&mut buffer).unwrap();
/// ```
pub trait KafkaDecodable where Self: Sized {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<Self>;
}
