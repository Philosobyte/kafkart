use anyhow::Result;
use std::fmt::Debug;
use std::io::{Read, Write};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes::buf::{Reader, Writer};
use kafka_encode::{KafkaDecodable, KafkaEncodable};
use kafka_encode::primitives::{CompactString, NullableString, VarArray};
use crate::protocol::api_key::ApiKey::ApiVersions;
use crate::protocol::api_versions::ApiVersionsRequestV3;
use crate::protocol::headers::RequestHeaderV2;
use crate::protocol::tags::TaggedFields;

pub struct PairWithI32EncodedSize<T1, T2>(T1, T2) where
    T1: KafkaEncodable + KafkaDecodable + Debug,
    T2: KafkaEncodable + KafkaDecodable + Debug;

impl<T1, T2> KafkaEncodable for PairWithI32EncodedSize<T1, T2> where
    T1: KafkaEncodable + KafkaDecodable + Debug,
    T2: KafkaEncodable + KafkaDecodable + Debug {

    fn to_kafka_bytes<W: Write + Debug>(self, writer: &mut W) -> Result<()> {
        let mut buffer_writer: Writer<BytesMut> = BytesMut::new().writer();
        self.0.to_kafka_bytes(&mut buffer_writer)?;
        self.1.to_kafka_bytes(&mut buffer_writer)?;

        let buffer: Bytes = buffer_writer.into_inner().freeze();

        (buffer.len() as i32).to_kafka_bytes(writer)?;
        writer.write_all(buffer.as_ref())?;
        Ok(())
    }
}

impl<T1, T2> KafkaDecodable for PairWithI32EncodedSize<T1, T2> where
    T1: KafkaEncodable + KafkaDecodable + Debug,
    T2: KafkaEncodable + KafkaDecodable + Debug {

    fn from_kafka_bytes<R: Read + Debug>(reader: &mut R) -> Result<Self> {
        let size: i32 = i32::from_kafka_bytes(reader)?;
        let pair: PairWithI32EncodedSize<T1, T2> = PairWithI32EncodedSize(
            T1::from_kafka_bytes(reader)?,
            T2::from_kafka_bytes(reader)?
        );
        Ok(pair)
    }
}

#[test]
fn test_encode_request() {
    let request_header: RequestHeaderV2 = RequestHeaderV2 {
        request_api_key: ApiVersions,
        request_api_version: 3,
        correlation_id: 0,
        client_id: NullableString(Some(String::from("producer-1"))),
        tag_buffer: TaggedFields { tags: VarArray(Vec::new()) },
    };
    let request_body: ApiVersionsRequestV3 = ApiVersionsRequestV3 {
        client_software_name: CompactString(String::from("apache-kafka-java")),
        client_software_version: CompactString(String::from("3.3.1")),
        tag_buffer: TaggedFields { tags: VarArray(Vec::new()) },
    };
    let request: PairWithI32EncodedSize<RequestHeaderV2, ApiVersionsRequestV3> = PairWithI32EncodedSize(request_header, request_body);

    let mut writer = BytesMut::new().writer();
    request.to_kafka_bytes(&mut writer).unwrap();
    assert_eq!(
        writer.into_inner().freeze().to_vec(),
        vec![0, 0, 0, 46, 0, 18, 0, 3, 0, 0, 0, 0, 0, 10, 112,
             114, 111, 100, 117, 99, 101, 114, 45, 49, 0, 18,
             97, 112, 97, 99, 104, 101, 45, 107, 97, 102, 107,
             97, 45, 106, 97, 118, 97, 6, 51, 46, 51, 46, 49, 0]
    );
}

#[test]
fn test_decode_request() {
    let mut reader: Reader<Bytes> = Bytes::from(
        vec![0, 0, 0, 46, 0, 18, 0, 3, 0, 0, 0, 0, 0, 10, 112,
             114, 111, 100, 117, 99, 101, 114, 45, 49, 0, 18,
             97, 112, 97, 99, 104, 101, 45, 107, 97, 102, 107,
             97, 45, 106, 97, 118, 97, 6, 51, 46, 51, 46, 49, 0]
    ).reader();

    let request = PairWithI32EncodedSize::<RequestHeaderV2, ApiVersionsRequestV3>::from_kafka_bytes(&mut reader).unwrap();
    assert_eq!(
        request.0,
        RequestHeaderV2 {
            request_api_key: ApiVersions,
            request_api_version: 3,
            correlation_id: 0,
            client_id: NullableString(Some(String::from("producer-1"))),
            tag_buffer: TaggedFields { tags: VarArray(Vec::new()) },
        }
    );
    assert_eq!(
        request.1,
        ApiVersionsRequestV3 {
            client_software_name: CompactString(String::from("apache-kafka-java")),
            client_software_version: CompactString(String::from("3.3.1")),
            tag_buffer: TaggedFields { tags: VarArray(Vec::new()) },
        }
    );
}


// #[test_log::test]
// fn test_encode_response() {
//     let request_header: RequestHeaderV2 = RequestHeaderV2 {
//         request_api_key: ApiVersions,
//         request_api_version: 3,
//         correlation_id: 0,
//         client_id: NullableString(Some(String::from("producer-1"))),
//         tag_buffer: TagBuffer { tags: VarArray(Vec::new()) },
//     };
//     let request_body: ApiVersionsRequestV3 = ApiVersionsRequestV3 {
//         client_software_name: CompactString(String::from("apache-kafka-java")),
//         client_software_version: CompactString(String::from("3.3.1")),
//         tag_buffer: TagBuffer { tags: VarArray(Vec::new()) },
//     };
//     let request: PairWithI32EncodedSize<RequestHeaderV2, ApiVersionsRequestV3> = PairWithI32EncodedSize(request_header, request_body);
//
//     let mut writer = BytesMut::new().writer();
//     request.to_kafka_bytes(&mut writer).unwrap();
//     assert_eq!(
//         writer.into_inner().freeze().to_vec(),
//         vec![0, 0, 0, 0, 0, 0, 0, 60, 0, 0, 0, 0, 0, 9, 0,
//              0, 1, 0, 0, 0, 13, 0, 0, 2, 0, 0, 0, 7, 0,
//              0, 3, 0, 0, 0, 12, 0, 0, 4, 0, 0, 0, 6, 0,
//              0, 5, 0, 0, 0, 3, 0, 0, 6, 0, 0, 0, 7, 0,
//              0, 7, 0, 0, 0, 3, 0, 0, 8, 0, 0, 0, 8, 0,
//              0, 9, 0, 0, 0, 8, 0, 0, 10, 0, 0, 0, 4, 0,
//              0, 11, 0, 0, 0, 9, 0, 0, 12, 0, 0, 0, 4, 0,
//              0, 13, 0, 0, 0, 5, 0, 0, 14, 0, 0, 0, 5, 0,
//              0, 15, 0, 0, 0, 5, 0, 0, 16, 0, 0, 0, 4, 0,
//              0, 17, 0, 0, 0, 1, 0, 0, 18, 0, 0, 0, 3, 0,
//              0, 19, 0, 0, 0, 7, 0, 0, 20, 0, 0, 0, 6, 0,
//              0, 21, 0, 0, 0, 2, 0, 0, 22, 0, 0, 0, 4, 0,
//              0, 23, 0, 0, 0, 4, 0, 0, 24, 0, 0, 0, 3, 0,
//              0, 25, 0, 0, 0, 3, 0, 0, 26, 0, 0, 0, 3, 0,
//              0, 27, 0, 0, 0, 1, 0, 0, 28, 0, 0, 0, 3, 0,
//              0, 29, 0, 0, 0, 3, 0, 0, 30, 0, 0, 0, 3, 0,
//              0, 31, 0, 0, 0, 3, 0, 0, 32, 0, 0, 0, 4, 0,
//              0, 33, 0, 0, 0, 2, 0, 0, 34, 0, 0, 0, 2, 0,
//              0, 35, 0, 0, 0, 4, 0, 0, 36, 0, 0, 0, 2, 0,
//              0, 37, 0, 0, 0, 3, 0, 0, 38, 0, 0, 0, 3, 0,
//              0, 39, 0, 0, 0, 2, 0, 0, 40, 0, 0, 0, 2, 0,
//              0, 41, 0, 0, 0, 3, 0, 0, 42, 0, 0, 0, 2, 0,
//              0, 43, 0, 0, 0, 2, 0, 0, 44, 0, 0, 0, 1, 0,
//              0, 45, 0, 0, 0, 0, 0, 0, 46, 0, 0, 0, 0, 0,
//              0, 47, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 1, 0,
//              0, 49, 0, 0, 0, 1, 0, 0, 50, 0, 0, 0, 0, 0,
//              0, 51, 0, 0, 0, 0, 0, 0, 56, 0, 0, 0, 2, 0,
//              0, 57, 0, 0, 0, 1, 0, 0, 60, 0, 0, 0, 0, 0,
//              0, 61, 0, 0, 0, 0, 0, 0, 65, 0, 0, 0, 0, 0,
//              0, 66, 0, 0, 0, 0, 0, 0, 67, 0, 0, 0, 0, 0,
//              0, 0, 0, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0]
//     );
// }