use anyhow::{anyhow, Result};
use std::fmt::Debug;
use std::io::{Read, Write, ErrorKind, Error};
use integer_encoding::{VarIntReader, VarIntWriter};
use tracing::{instrument, trace};
use uuid::Uuid;
use crate::{KafkaDecodable, KafkaEncodable};
use crate::primitives::{NullableArray, CompactNullableArray, CompactBytes, CompactNullableBytes, CompactNullableString, CompactString, NullableBytes, NullableString, UnsignedVarInt32, VarI32, VarI64, Array, CompactArray, VarArray};

// BOOLEAN
impl KafkaEncodable for bool {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = &[self as u8];
        trace!("bool bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for bool {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<bool> {
        let mut buf: [u8; 1] = [0; 1];
        reader.read_exact(&mut buf)?;

        trace!("bool bytes: {:?}", buf);
        Ok(buf[0] != 0u8)
    }
}

// UINT8
impl KafkaEncodable for u8 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = &self.to_be_bytes();
        trace!("u8 bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for u8 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<u8> {
        let mut buf: [u8; 1] = [0; 1];
        reader.read_exact(&mut buf)?;

        trace!("u8 bytes: {:?}", buf);
        Ok(buf[0])
    }
}

// INT8
impl KafkaEncodable for i8 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = &self.to_be_bytes();
        trace!("i8 bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for i8 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<i8> {
        let mut buf: [u8; 1] = [0; 1];
        reader.read_exact(&mut buf)?;

        trace!("i8 bytes: {:?}", buf);
        Ok(buf[0] as i8)
    }
}

// INT16
impl KafkaEncodable for i16 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = &self.to_be_bytes();
        trace!("i16 bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for i16 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<i16> {
        let mut buf: [u8; 2] = [0; 2];
        reader.read_exact(&mut buf)?;

        trace!("i16 bytes: {:?}", buf);
        Ok(i16::from_be_bytes(buf))
    }
}

// INT32
impl KafkaEncodable for i32 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = &self.to_be_bytes();
        trace!("i32 bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for i32 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<i32> {
        let mut buf: [u8; 4] = [0; 4];
        reader.read_exact(&mut buf)?;

        trace!("i32 bytes: {:?}", buf);
        Ok(i32::from_be_bytes(buf))
    }
}

// INT64
impl KafkaEncodable for i64 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = &self.to_be_bytes();
        trace!("i64 bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for i64 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<i64> {
        let mut buf: [u8; 8] = [0; 8];
        reader.read_exact(&mut buf)?;

        trace!("i64 bytes: {:?}", buf);
        Ok(i64::from_be_bytes(buf))
    }
}

// UINT32
impl KafkaEncodable for u32 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = &self.to_be_bytes();
        trace!("u32 bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for u32 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<u32> {
        let mut buf: [u8; 4] = [0; 4];
        reader.read_exact(&mut buf)?;

        trace!("u32 bytes: {:?}", buf);
        Ok(u32::from_be_bytes(buf))
    }
}

// VARINT
impl KafkaEncodable for VarI32 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let mut bytes_vec: Vec<u8> = Vec::new();
        bytes_vec.write_varint(*self)?;

        trace!("VarI32 bytes: {:?}", bytes_vec);
        writer.write_all(&*bytes_vec);
        Ok(())
    }
}

impl KafkaDecodable for VarI32 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<VarI32> {
        let var_i32: i32 = reader.read_varint()?;
        trace!(var_i32);

        Ok(VarI32(var_i32))
    }
}

// VARLONG
impl KafkaEncodable for VarI64 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let mut bytes_vec: Vec<u8> = Vec::new();
        bytes_vec.write_varint(*self)?;

        trace!("VarI64 bytes: {:?}", bytes_vec);
        writer.write_all(&*bytes_vec);
        Ok(())
    }
}

impl KafkaDecodable for VarI64 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<VarI64> {
        let var_i64: i64 = reader.read_varint()?;
        trace!(var_i64);

        Ok(VarI64(var_i64))
    }
}

// UUID
impl KafkaEncodable for Uuid {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = self.as_bytes().as_slice();
        trace!("uuid bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for Uuid {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<Uuid> {
        let mut buf: [u8; 16] = [0; 16];
        reader.read_exact(&mut buf)?;

        trace!("uuid bytes: {:?}", buf);
        Ok(Uuid::from_bytes(buf))
    }
}

// FLOAT64
impl KafkaEncodable for f64 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = &self.to_be_bytes();
        trace!("f64 bytes: {:?}", bytes);

        writer.write_all(bytes)?;
        Ok(())
    }
}

impl KafkaDecodable for f64 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<f64> {
        let mut buf: [u8; 8] = [0; 8];
        reader.read_exact(&mut buf)?;

        trace!("f64 bytes: {:?}", buf);
        Ok(f64::from_be_bytes(buf))
    }
}

// serialize and deserialize a byte slice such that the length of the byte slice is encoded before the contents of the slice.
// The size type may be specified as any integer size, e.g. i16 or i32
macro_rules! write_bytes_with_size_header {
    ($writer:expr, $bytes_to_write:expr, $size_type:ty) => {
        {
            if $bytes_to_write.len() >= <$size_type>::MAX as usize {
                return Err(anyhow!("Too many bytes: {}", $bytes_to_write.len()).into());
            }
            ($bytes_to_write.len() as $size_type).to_kafka_bytes($writer)?;
            $writer.write_all($bytes_to_write)?;
        }
    }
}

macro_rules! read_bytes_with_size_header {
    ($reader:expr, $size_type:ty, $buf_transform_argument:ident, $buf_transform:expr) => {
        {
            let bytes_length: $size_type = <$size_type>::from_kafka_bytes($reader)?;
            let mut $buf_transform_argument: Vec<u8> = vec![0; bytes_length as usize];
            $reader.read_exact(&mut *$buf_transform_argument)?;
            match $buf_transform {
                Ok(output) => Ok(output),
                Err(e) => Err(e.into())
            }
        }
    }
}

// serialize and deserialize a byte slice such that the length of the byte slice is encoded
// as an UnsignedVarInt32 before the contents of the slice.
fn write_bytes_with_unsigned_varint_size_header<W: Write + Debug>(writer: &mut W, bytes_to_write: &[u8]) -> Result<()> {
    let bytes_length: usize = bytes_to_write.len() + 1;

    UnsignedVarInt32(bytes_length as u32).to_kafka_bytes(writer)?;
    writer.write_all(bytes_to_write)?;
    Ok(())
}

fn read_bytes_with_unsigned_varint_size_header<R: Read + Debug>(reader: &mut R) -> Result<Vec<u8>> {
    let target_bytes_length: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;

    let mut target_bytes: Vec<u8> = vec![0; (*target_bytes_length - 1) as usize];
    reader.read_exact(&mut *target_bytes)?;
    Ok(target_bytes)
}

// STRING
impl KafkaEncodable for String {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = self.as_bytes();
        trace!("String bytes: {:?}", bytes);

        write_bytes_with_size_header!(writer, bytes, i16);
        Ok(())
    }
}

impl KafkaDecodable for String {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<String> {
        read_bytes_with_size_header!(
            reader,
            i16,
            buf,
            {
                trace!("String bytes: {:?}", buf);
                String::from_utf8(buf)
            }
        )
    }
}

// UNSIGNED_VARINT
impl KafkaEncodable for UnsignedVarInt32 {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.write_varint(*self)?;
        trace!("UnsignedVarInt32 bytes: {:?}", bytes);

        writer.write_all(&*bytes);
        Ok(())
    }
}

impl KafkaDecodable for UnsignedVarInt32 {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<UnsignedVarInt32> {
        let var_u32: u32 = reader.read_varint()?;
        trace!(var_u32);

        Ok(UnsignedVarInt32(var_u32))
    }
}

// COMPACT_STRING
impl KafkaEncodable for CompactString {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = self.as_bytes();
        trace!("CompactString bytes: {:?}", bytes);
        write_bytes_with_unsigned_varint_size_header(writer, bytes)
    }
}

impl KafkaDecodable for CompactString {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<CompactString> {
        let bytes: Vec<u8> = read_bytes_with_unsigned_varint_size_header(reader)?;
        trace!("CompactString bytes: {:?}", bytes);

        match String::from_utf8(bytes) {
            Ok(s) => Ok(CompactString(s)),
            Err(e) => Err(e.into())
        }
    }
}

// NULLABLE_STRING
impl KafkaEncodable for NullableString {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        if self.is_none() {
            return (-1_i16).to_kafka_bytes(writer);
        }

        let string: String = self.0.unwrap();
        let bytes: &[u8] = string.as_bytes();
        trace!("NullableString bytes: {:?}", bytes);

        write_bytes_with_size_header!(writer, bytes, i16);
        Ok(())
    }
}

impl KafkaDecodable for NullableString {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<NullableString> {
        let bytes_length: i16 = i16::from_kafka_bytes(reader)?;
        if bytes_length == -1_i16 {
            return Ok(NullableString(None));
        }

        let mut bytes: Vec<u8> = vec![0; bytes_length as usize];
        reader.read_exact(&mut *bytes)?;
        trace!("NullableString bytes: {:?}", bytes);

        match String::from_utf8(bytes) {
            Ok(s) => Ok(NullableString(Some(s))),
            Err(e) => Err(e.into())
        }
    }
}

// COMPACT_NULLABLE_STRING
impl KafkaEncodable for CompactNullableString {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(mut self, writer: &mut W) -> Result<()> {
        if self.is_none() {
            return UnsignedVarInt32(0u32).to_kafka_bytes(writer);
        }
        // must write the data size first as an UnsignedVarInt32
        let bytes: &[u8] = self.as_mut().unwrap().as_bytes();
        write_bytes_with_unsigned_varint_size_header(writer, bytes)
    }
}

impl KafkaDecodable for CompactNullableString {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<CompactNullableString> {
        let bytes_length: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;

        if (*bytes_length) == 0u32 {
            return Ok(CompactNullableString(None));
        }
        let mut bytes: Vec<u8> = vec![0; (*bytes_length - 1) as usize];
        reader.read_exact(&mut *bytes)?;
        trace!("CompactNullableString bytes: {:?}", bytes);

        match String::from_utf8(bytes) {
            Ok(s) => Ok(CompactNullableString(Some(s))),
            Err(e) => Err(e.into())
        }
    }
}

// BYTES
impl KafkaEncodable for Vec<u8> {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        trace!("Vec<u8> bytes: {:?}", &*self);
        write_bytes_with_size_header!(writer, &*self, i32);
        Ok(())
    }
}

impl KafkaDecodable for Vec<u8> {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<Vec<u8>> {
        read_bytes_with_size_header!(
            reader, i32, buf,
            {
                trace!("Vec<u8> bytes: {:?}", buf);
                Ok::<Vec<u8>, Error>(buf.to_vec())
            }
        )
    }
}

// COMPACT_BYTES
impl KafkaEncodable for CompactBytes {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let bytes: &[u8] = self.as_slice();
        trace!("CompactBytes bytes: {:?}", bytes);
        write_bytes_with_unsigned_varint_size_header(writer, bytes)
    }
}

impl KafkaDecodable for CompactBytes {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<CompactBytes> {
        let bytes: Vec<u8> = read_bytes_with_unsigned_varint_size_header(reader)?;
        trace!("CompactBytes bytes: {:?}", bytes);
        Ok(CompactBytes(bytes))
    }
}

// NULLABLE_BYTES
impl KafkaEncodable for NullableBytes {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        if self.is_none() {
            return (-1_i32).to_kafka_bytes(writer);
        }

        let bytes: &[u8] = &*self.0.unwrap();
        trace!("NullableBytes bytes: {:?}", bytes);

        write_bytes_with_size_header!(writer, bytes, i32);
        Ok(())
    }
}

impl KafkaDecodable for NullableBytes {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<NullableBytes> {
        let bytes_length: i32 = i32::from_kafka_bytes(reader)?;
        if bytes_length == -1_i32 {
            return Ok(NullableBytes(None));
        }

        let mut bytes: Vec<u8> = vec![0; bytes_length as usize];
        reader.read_exact(&mut *bytes)?;

        trace!("NullableBytes bytes: {:?}", bytes);
        Ok(NullableBytes(Some(bytes)))
    }
}

// COMPACT_NULLABLE_BYTES
impl KafkaEncodable for CompactNullableBytes {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(mut self, writer: &mut W) -> Result<()> {
        if self.is_none() {
            return UnsignedVarInt32(0u32).to_kafka_bytes(writer);
        }

        let bytes: &[u8] = &*self.as_mut().unwrap();
        trace!("CompactNullableBytes bytes: {:?}", bytes);
        write_bytes_with_unsigned_varint_size_header(writer, bytes)
    }
}

impl KafkaDecodable for CompactNullableBytes {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<CompactNullableBytes> {
        let bytes_length: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;

        if (*bytes_length) == 0u32 {
            return Ok(CompactNullableBytes(None));
        }

        let mut bytes: Vec<u8> = vec![0; (*bytes_length - 1) as usize];
        reader.read_exact(&mut *bytes)?;

        trace!("CompactNullableByets bytes: {:?}", bytes);
        Ok(CompactNullableBytes(Some(bytes)))
    }
}

// ARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for Array<T> {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let elements: Vec<T> = self.0;
        let num_elements: i32 = elements.len() as i32;
        trace!(num_elements);

        num_elements.to_kafka_bytes(writer)?;

        let mut bytes: Vec<u8> = Vec::new();
        for element in elements {
            trace!("element: {:?}", element);

            element.to_kafka_bytes(&mut bytes)?;
            trace!("element bytes: {:?}", bytes);

            writer.write_all(&*bytes);
            bytes.clear();
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for Array<T> {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<Array<T>> {
        let num_elements: i32 = i32::from_kafka_bytes(reader)?;
        trace!(num_elements);

        let mut elements: Vec<T> = Vec::new();
        for _ in 0..num_elements {
            let element: T = T::from_kafka_bytes(reader)?;
            trace!("element: {:?}", element);
            elements.push(element);
        }
        Ok(Array::<T>::new(elements))
    }
}

// NULLABLE_ARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for NullableArray<T> {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        if self.is_none() {
            return (-1_i32).to_kafka_bytes(writer);
        }

        let elements: Vec<T> = self.0.unwrap();
        let num_elements: i32 = elements.len() as i32;
        trace!(num_elements);
        num_elements.to_kafka_bytes(writer)?;

        let mut bytes: Vec<u8> = Vec::new();
        for element in elements {
            trace!("element: {:?}", element);

            element.to_kafka_bytes(&mut bytes)?;
            trace!("element bytes: {:?}", bytes);

            writer.write_all(&*bytes);
            bytes.clear();
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for NullableArray<T> {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<NullableArray<T>> {
        let num_elements: i32 = i32::from_kafka_bytes(reader)?;
        trace!(num_elements);
        if num_elements == -1_i32 {
            return Ok(NullableArray::<T>::new(None));
        }

        let mut elements: Vec<T> = Vec::new();

        for _ in 0..num_elements {
            let element: T = T::from_kafka_bytes(reader)?;
            trace!("element: {:?}", element);
            elements.push(element);
        }
        Ok(NullableArray::<T>::new(Some(elements)))
    }
}

// COMPACT_ARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for CompactArray<T> {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let elements: Vec<T> = self.0;
        let num_elements: u32 = elements.len() as u32;
        trace!(num_elements);

        UnsignedVarInt32(num_elements + 1u32).to_kafka_bytes(writer)?;

        let mut bytes: Vec<u8> = Vec::new();
        for element in elements {
            trace!("element: {:?}", element);

            element.to_kafka_bytes(&mut bytes)?;
            trace!("element bytes: {:?}", bytes);

            writer.write_all(&*bytes);
            bytes.clear();
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for CompactArray<T> {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<CompactArray<T>> {
        let num_elements_plus_one: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
        let num_elements: u32 = num_elements_plus_one.0 - 1u32;

        trace!(num_elements);
        let mut elements: Vec<T> = Vec::new();

        for _ in 0..num_elements {
            let element: T = T::from_kafka_bytes(reader)?;
            trace!("element: {:?}", element);
            elements.push(element);
        }
        Ok(CompactArray::<T>::new(elements))
    }
}

// VARARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for VarArray<T> {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let elements: Vec<T> = self.0;

        let num_elements: u32 = elements.len() as u32;
        trace!(num_elements);
        UnsignedVarInt32(num_elements).to_kafka_bytes(writer)?;

        let mut bytes: Vec<u8> = Vec::new();
        for element in elements {
            trace!("element: {:?}", element);

            element.to_kafka_bytes(&mut bytes)?;
            trace!("element bytes: {:?}", bytes);

            writer.write_all(&*bytes);
            bytes.clear();
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for VarArray<T> {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<VarArray<T>> {
        let num_elements_varint: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
        let num_elements: u32 = num_elements_varint.0;
        trace!(num_elements);

        let mut elements: Vec<T> = Vec::new();
        for _ in 0..num_elements {
            let element: T = T::from_kafka_bytes(reader)?;
            trace!("element: {:?}", element);
            elements.push(element);
        }
        Ok(VarArray::<T>::new(elements))
    }
}

// COMPACT_NULLABLE_ARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for CompactNullableArray<T> {
    #[instrument]
    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        if self.is_none() {
            return UnsignedVarInt32(0).to_kafka_bytes(writer);
        }

        let elements: Vec<T> = self.0.unwrap();
        let num_elements: u32 = elements.len() as u32;
        trace!(num_elements);
        UnsignedVarInt32(num_elements + 1u32).to_kafka_bytes(writer)?;

        let mut bytes: Vec<u8> = Vec::new();
        for element in elements {
            trace!("element: {:?}", element);

            element.to_kafka_bytes(&mut bytes)?;
            trace!("element bytes: {:?}", bytes);

            writer.write_all(&*bytes);
            bytes.clear();
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for CompactNullableArray<T> {
    #[instrument]
    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<CompactNullableArray<T>> {
        let num_elements_varint: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
        let num_elements: u32 = num_elements_varint.0;
        trace!(num_elements);

        if num_elements == 0u32 {
            return Ok(CompactNullableArray::<T>::new(None));
        }

        let mut elements: Vec<T> = Vec::new();
        for _ in 0..num_elements - 1u32 {
            let element: T = T::from_kafka_bytes(reader)?;
            trace!("element: {:?}", element);
            elements.push(element);
        }
        Ok(CompactNullableArray::<T>::new(Some(elements)))
    }
}
