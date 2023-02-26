use std::fmt::Debug;
use std::io::{Read, Write, Result, ErrorKind, Error, Seek};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::{VarIntReader, VarIntWriter};
use log::{trace};
use uuid::Uuid;
use crate::{KafkaDecodable, KafkaEncodable};
use crate::primitives::{NullableArray, CompactNullableArray, CompactBytes, CompactNullableBytes, CompactNullableString, CompactString, NullableBytes, NullableString, UnsignedVarInt32, VarI32, VarI64, Array, CompactArray, VarArray};
use crate::err::TooManyBytesError;

// BOOLEAN
impl KafkaEncodable for bool {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing bool {:?}", self);
        writer.write_all(&[self as u8])
    }
}

impl KafkaDecodable for bool {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<bool> {
        trace!("Attempting to read bool");

        let mut buf: [u8; 1] = [0; 1];
        reader.read_exact(&mut buf)?;
        let b: bool = buf[0] != 0u8;

        trace!("Read bool {:?}", b);
        Ok(b)
    }
}

// UINT8
impl KafkaEncodable for u8 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing u8 {:?}", self);
        writer.write_all(&self.to_be_bytes())
    }
}

impl KafkaDecodable for u8 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<u8> {
        trace!("Attempting to read u8");

        let mut buf: [u8; 1] = [0; 1];
        reader.read_exact(&mut buf)?;
        let u: u8 = buf[0];

        trace!("Read u8 {:?}", u);
        Ok(u)
    }
}

// INT8
impl KafkaEncodable for i8 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing i8 {:?}", self);
        writer.write_all(&self.to_be_bytes())
    }
}

impl KafkaDecodable for i8 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<i8> {
        trace!("Attempting to read i8");

        let mut buf: [u8; 1] = [0; 1];
        reader.read_exact(&mut buf)?;
        let i: i8 = buf[0] as i8;

        trace!("Read i8 {:?}", i);
        Ok(i)
    }
}

// INT16
impl KafkaEncodable for i16 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing i16 {:?}", self);
        writer.write_all(&self.to_be_bytes())
    }
}

impl KafkaDecodable for i16 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<i16> {
        trace!("Attempting to read i16");
        let mut buf: [u8; 2] = [0; 2];
        reader.read_exact(&mut buf)?;
        let i: i16 = i16::from_be_bytes(buf);

        trace!("Read i16 {:?}", i);
        Ok(i)
    }
}

// INT32
impl KafkaEncodable for i32 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing i32 {:?}", self);
        writer.write_all(&self.to_be_bytes())
    }
}

impl KafkaDecodable for i32 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<i32> {
        trace!("Attempting to read i32");
        let mut buf: [u8; 4] = [0; 4];
        reader.read_exact(&mut buf)?;
        let i: i32 = i32::from_be_bytes(buf);

        trace!("Read i32 {:?}", i);
        Ok(i)
    }
}

// INT64
impl KafkaEncodable for i64 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing i64 {:?}", self);
        writer.write_all(&self.to_be_bytes())
    }
}

impl KafkaDecodable for i64 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<i64> {
        trace!("Attempting to read i64");
        let mut buf: [u8; 8] = [0; 8];
        reader.read_exact(&mut buf)?;
        let i: i64 = i64::from_be_bytes(buf);

        trace!("Read i64 {:?}", i);
        Ok(i)
    }
}

// UINT32
impl KafkaEncodable for u32 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing u32 {:?}", self);
        writer.write_all(&self.to_be_bytes())
    }
}

impl KafkaDecodable for u32 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<u32> {
        trace!("Attempting to read u32");
        let mut buf: [u8; 4] = [0; 4];
        reader.read_exact(&mut buf)?;
        let u: u32 = u32::from_be_bytes(buf);

        trace!("Read u32 {:?}", u);
        Ok(u)
    }
}

// VARINT


impl KafkaEncodable for VarI32 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing VarI32 {:?}", self);
        writer.write_varint(*self)?;
        Ok(())
    }
}

impl KafkaDecodable for VarI32 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<VarI32> {
        trace!("Attempting to read VarI32");
        reader.read_varint().map(|var_i32| VarI32(var_i32))
    }
}

// VARLONG
impl KafkaEncodable for VarI64 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing VarI64 {:?}", self);
        writer.write_varint(*self)?;
        Ok(())
    }
}

impl KafkaDecodable for VarI64 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<VarI64> {
        trace!("Attempting to read VarI64");
        reader.read_varint().map(|var_i64| VarI64(var_i64))
    }
}

// UUID
impl KafkaEncodable for Uuid {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing Uuid {:?}", self);
        writer.write_all(self.as_bytes().as_slice())
    }
}

impl KafkaDecodable for Uuid {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<Uuid> {
        trace!("Attempting to read Uuid");
        let mut buf: [u8; 16] = [0; 16];
        reader.read_exact(&mut buf)?;
        Ok(Uuid::from_bytes(buf))
    }
}

// FLOAT64
impl KafkaEncodable for f64 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing f64 {:?}", self);
        writer.write_all(&self.to_be_bytes())
    }
}

impl KafkaDecodable for f64 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<f64> {
        trace!("Attempting to read f64");
        let mut buf: [u8; 8] = [0; 8];
        reader.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }
}

// serialize and deserialize a byte slice such that the length of the byte slice is encoded before the contents of the slice.
// The size type may be specified as any integer size, e.g. i16 or i32
macro_rules! write_bytes_with_size_header {
    ($writer:expr, $bytes_to_write:expr, $size_type:ty) => {
        {
            if $bytes_to_write.len() >= <$size_type>::MAX as usize {
                return Err(Error::new(ErrorKind::InvalidData, TooManyBytesError { size: $bytes_to_write.len() }));
            }
            ($bytes_to_write.len() as $size_type).to_kafka_bytes($writer)?;
            return $writer.write_all($bytes_to_write);
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
                Err(e) => Err(Error::new(ErrorKind::InvalidData, e))
            }
        }
    }
}

// serialize and deserialize a byte slice such that the length of the byte slice is encoded
// as an UnsignedVarInt32 before the contents of the slice.
fn write_bytes_with_unsigned_varint_size_header<W: Write>(writer: &mut W, bytes_to_write: &[u8]) -> Result<()> {
    let bytes_length: usize = bytes_to_write.len() + 1;

    UnsignedVarInt32(bytes_length as u32).to_kafka_bytes(writer)?;
    writer.write_all(bytes_to_write)
}

fn read_bytes_with_unsigned_varint_size_header<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let target_bytes_length: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;

    let mut target_bytes: Vec<u8> = vec![0; (*target_bytes_length - 1) as usize];
    reader.read_exact(&mut *target_bytes)?;
    Ok(target_bytes)
}

// STRING
impl KafkaEncodable for String {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing String {:?}", self);
        let bytes: &[u8] = self.as_bytes();
        write_bytes_with_size_header!(writer, bytes, i16)
    }
}

impl KafkaDecodable for String {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<String> {
        trace!("Attempting to read String");
        read_bytes_with_size_header!(reader, i16, buf, String::from_utf8(buf))
    }
}

// UNSIGNED_VARINT
impl KafkaEncodable for UnsignedVarInt32 {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing UnsignedVarInt32 {:?}", self);
        writer.write_varint(*self)?;
        Ok(())
    }
}

impl KafkaDecodable for UnsignedVarInt32 {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<UnsignedVarInt32> {
        trace!("Attempting to read UnsignedVarInt32");
        reader.read_varint().map(|var_u32| UnsignedVarInt32(var_u32))
    }
}

// COMPACT_STRING
impl KafkaEncodable for CompactString {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing CompactString {:?}", self);
        let bytes: &[u8] = self.as_bytes();
        write_bytes_with_unsigned_varint_size_header(writer, bytes)
    }
}

impl KafkaDecodable for CompactString {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<CompactString> {
        trace!("Attempting to read CompactString");
        let bytes: Vec<u8> = read_bytes_with_unsigned_varint_size_header(reader)?;

        match String::from_utf8(bytes) {
            Ok(s) => Ok(CompactString(s)),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e))
        }
    }
}

// NULLABLE_STRING
impl KafkaEncodable for NullableString {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing NullableString {:?}", self);
        if self.is_none() {
            return (-1_i16).to_kafka_bytes(writer);
        }

        let string: String = self.0.unwrap();
        let bytes: &[u8] = string.as_bytes();

        write_bytes_with_size_header!(writer, bytes, i16)
    }
}

impl KafkaDecodable for NullableString {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<NullableString> {
        trace!("Attempting to read NullableString");
        let bytes_length: i16 = i16::from_kafka_bytes(reader)?;
        if bytes_length == -1_i16 {
            return Ok(NullableString(None));
        }

        let mut bytes: Vec<u8> = vec![0; bytes_length as usize];
        reader.read_exact(&mut *bytes)?;
        match String::from_utf8(bytes) {
            Ok(s) => Ok(NullableString(Some(s))),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e))
        }
    }
}

// COMPACT_NULLABLE_STRING
impl KafkaEncodable for CompactNullableString {
    fn to_kafka_bytes<W: Write>(mut self, writer: &mut W) -> Result<()> {
        trace!("Writing CompactNullableString {:?}", self);
        if self.is_none() {
            return UnsignedVarInt32(0u32).to_kafka_bytes(writer);
        }
        // must write the data size first as an UnsignedVarInt32
        let bytes: &[u8] = self.as_mut().unwrap().as_bytes();
        write_bytes_with_unsigned_varint_size_header(writer, bytes)
    }
}

impl KafkaDecodable for CompactNullableString {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<CompactNullableString> {
        trace!("Attempting to read CompactNullableString");
        let bytes_length: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;

        if (*bytes_length) == 0u32 {
            return Ok(CompactNullableString(None));
        }

        let mut bytes: Vec<u8> = vec![0; (*bytes_length - 1) as usize];
        reader.read_exact(&mut *bytes)?;
        match String::from_utf8(bytes) {
            Ok(s) => Ok(CompactNullableString(Some(s))),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e))
        }
    }
}

// BYTES
impl KafkaEncodable for Vec<u8> {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing Vec<u8> {:?}", self);
        write_bytes_with_size_header!(writer, &*self, i32)
    }
}

impl KafkaDecodable for Vec<u8> {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
        trace!("Attempting to read Vec<u8>");
        read_bytes_with_size_header!(reader, i32, buf, Ok::<Vec<u8>, Error>(buf.to_vec()))
    }
}

// COMPACT_BYTES
impl KafkaEncodable for CompactBytes {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing CompactBytes {:?}", self);
        write_bytes_with_unsigned_varint_size_header(writer, self.as_slice())
    }
}

impl KafkaDecodable for CompactBytes {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<CompactBytes> {
        trace!("Attempting to read CompactBytes");
        let bytes: Vec<u8> = read_bytes_with_unsigned_varint_size_header(reader)?;
        Ok(CompactBytes(bytes))
    }
}

// NULLABLE_BYTES
impl KafkaEncodable for NullableBytes {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing NullableBytes {:?}", self);
        if self.is_none() {
            return (-1_i32).to_kafka_bytes(writer);
        }

        let bytes: &[u8] = &*self.0.unwrap();

        write_bytes_with_size_header!(writer, bytes, i32)
    }
}

impl KafkaDecodable for NullableBytes {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<NullableBytes> {
        trace!("Attempting to read NullableBytes");
        let bytes_length: i32 = i32::from_kafka_bytes(reader)?;
        if bytes_length == -1_i32 {
            return Ok(NullableBytes(None));
        }

        let mut bytes: Vec<u8> = vec![0; bytes_length as usize];
        reader.read_exact(&mut *bytes)?;
        Ok(NullableBytes(Some(bytes)))
    }
}

// COMPACT_NULLABLE_BYTES
impl KafkaEncodable for CompactNullableBytes {
    fn to_kafka_bytes<W: Write>(mut self, writer: &mut W) -> Result<()> {
        trace!("Writing CompactNullableBytes {:?}", self);
        if self.is_none() {
            return UnsignedVarInt32(0u32).to_kafka_bytes(writer);
        }

        let bytes: &[u8] = &*self.as_mut().unwrap();
        write_bytes_with_unsigned_varint_size_header(writer, bytes)
    }
}

impl KafkaDecodable for CompactNullableBytes {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<CompactNullableBytes> {
        trace!("Attempting to read CompactNullableBytes");
        let bytes_length: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;

        if (*bytes_length) == 0u32 {
            return Ok(CompactNullableBytes(None));
        }

        let mut bytes: Vec<u8> = vec![0; (*bytes_length - 1) as usize];
        reader.read_exact(&mut *bytes)?;
        Ok(CompactNullableBytes(Some(bytes)))
    }
}

// ARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for Array<T> {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing Array {:?}", self);
        let elements: Vec<T> = self.0;
        (elements.len() as i32).to_kafka_bytes(writer)?;
        for element in elements {
            element.to_kafka_bytes(writer)?;
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for Array<T> {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<Array<T>> {
        trace!("Attempting to read Array");
        let num_elements: i32 = i32::from_kafka_bytes(reader)?;
        let mut elements: Vec<T> = Vec::new();

        for _ in 0..num_elements {
            elements.push(T::from_kafka_bytes(reader)?);
        }
        Ok(Array::<T>::new(elements))
    }
}

// NULLABLE_ARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for NullableArray<T> {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing NullableArray {:?}", self);
        if self.is_none() {
            return (-1_i32).to_kafka_bytes(writer);
        }

        let elements: Vec<T> = self.0.unwrap();
        (elements.len() as i32).to_kafka_bytes(writer)?;
        for element in elements {
            element.to_kafka_bytes(writer)?;
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for NullableArray<T> {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<NullableArray<T>> {
        trace!("Attempting to read NullableArray");
        let num_elements: i32 = i32::from_kafka_bytes(reader)?;
        if num_elements == -1_i32 {
            return Ok(NullableArray::<T>::new(None));
        }

        let mut elements: Vec<T> = Vec::new();

        for _ in 0..num_elements {
            elements.push(T::from_kafka_bytes(reader)?);
        }
        Ok(NullableArray::<T>::new(Some(elements)))
    }
}

// COMPACT_ARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for CompactArray<T> {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing CompactArray {:?}", self);
        let elements: Vec<T> = self.0;
        UnsignedVarInt32(elements.len() as u32 + 1u32).to_kafka_bytes(writer)?;

        for element in elements {
            element.to_kafka_bytes(writer)?;
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for CompactArray<T> {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<CompactArray<T>> {
        trace!("Attempting to read CompactArray");
        let num_elements_plus_one: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
        let num_elements = num_elements_plus_one.0 - 1u32;

        trace!("num_elements {:?}", num_elements);
        let mut elements: Vec<T> = Vec::new();

        for i in 0..num_elements {
            let t: T = T::from_kafka_bytes(reader)?;
            trace!("{:?}th element: {:?}", i, t);
            elements.push(t);
        }
        Ok(CompactArray::<T>::new(elements))
    }
}

// VARARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for VarArray<T> {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing VarArray {:?}", self);
        let elements: Vec<T> = self.0;
        UnsignedVarInt32(elements.len() as u32).to_kafka_bytes(writer)?;

        for element in elements {
            element.to_kafka_bytes(writer)?;
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for VarArray<T> {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<VarArray<T>> {
        trace!("Attempting to read VarArray");
        let num_elements: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
        trace!("Found num_elements: {:?}", num_elements);

        let mut elements: Vec<T> = Vec::new();

        for i in 0..num_elements.0 {
            let t: T = T::from_kafka_bytes(reader)?;
            trace!("{:?}th element: {:?}", i, t);
            elements.push(t);
        }
        Ok(VarArray::<T>::new(elements))
    }
}

// COMPACT_NULLABLE_ARRAY
impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaEncodable for CompactNullableArray<T> {
    fn to_kafka_bytes<W: Write>(self, writer: &mut W) -> Result<()> {
        trace!("Writing CompactNullableArray {:?}", self);
        if self.is_none() {
            return UnsignedVarInt32(0).to_kafka_bytes(writer);
        }

        let elements: Vec<T> = self.0.unwrap();
        UnsignedVarInt32(elements.len() as u32 + 1u32).to_kafka_bytes(writer)?;
        for element in elements {
            element.to_kafka_bytes(writer)?;
        }
        Ok(())
    }
}

impl<T: KafkaEncodable + KafkaDecodable + Debug> KafkaDecodable for CompactNullableArray<T> {
    fn from_kafka_bytes<R: Read>(reader: &mut R) -> Result<CompactNullableArray<T>> {
        trace!("Attempting to read CompactNullableArray");
        let num_elements: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
        if num_elements.0 == 0u32 {
            return Ok(CompactNullableArray::<T>::new(None));
        }

        let mut elements: Vec<T> = Vec::new();

        for _ in 0..num_elements.0 - 1u32 {
            elements.push(T::from_kafka_bytes(reader)?);
        }
        Ok(CompactNullableArray::<T>::new(Some(elements)))
    }
}
