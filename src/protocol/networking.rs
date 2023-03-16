use std::fmt::Debug;
use ctor::ctor;
use std::io::{Read, Write};
use std::net::TcpStream;
use bytes::Buf;
use bytes::buf::Reader;
use tracing::{debug, info, instrument, Level, trace};
use tracing_subscriber::fmt::format;
use tracing_subscriber::fmt::format::FmtSpan;
use kafka_encode::KafkaEncodable;
use kafka_encode::primitives::{CompactString, NullableString, VarArray};
use anyhow::{anyhow, Result};
use rand::RngCore;
use rand::rngs::ThreadRng;
use crate::protocol::api_key::ApiKey;
use crate::protocol::api_versions::{ApiVersionsRequestV3, ApiVersionsResponseV3};
use crate::protocol::headers::{RequestHeaderV2, ResponseHeader, ResponseHeaderV0, ResponseHeaderV1};
use crate::protocol::{ApiVersion, KafkaRequest, KafkaResponse};
use crate::protocol::requests::PairWithI32EncodedSize;
use crate::protocol::tags::TaggedFields;

#[ctor]
fn print_spans_and_events_during_tests() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::DEBUG)
        // TODO: figure out how to make the writer to stdout not deadlock with multiple tests
        // .with_span_events(FmtSpan::ENTER)
        // .with_span_events(FmtSpan::EXIT)
        .event_format(format().pretty())
        .init();
}

fn deserialize_response_header_and_get_correlation_id<R: Read + Debug>(mut reader: &mut R, response_header_version: ApiVersion) -> Result<i32> {
    match response_header_version {
        0 => Ok(ResponseHeaderV0::from_kafka_bytes(&mut reader)?.correlation_id),
        1 => Ok(ResponseHeaderV1::from_kafka_bytes(&mut reader)?.correlation_id),
        any_other_version => Err(anyhow!("Unrecognized response header version: {}", any_other_version))
    }
}

#[derive(Debug)]
pub struct KafkaNetworkingClient<'a> {
    pub bootstrap_url: &'a str,
    pub client_id: &'a str,
    random: ThreadRng
}

impl<'a> KafkaNetworkingClient<'a> {
    fn new(bootstrap_url: &'a str, client_id: &'a str) -> Self {
        KafkaNetworkingClient {
            bootstrap_url,
            client_id,
            random: rand::thread_rng()
        }
    }

    #[instrument]
    fn send_request_and_get_response<Request: KafkaRequest, Response: KafkaResponse>(&mut self, request: Request) -> Result<Response> {
        let mut tcp_stream: TcpStream = TcpStream::connect(self.bootstrap_url)?;
        let correlation_id = self.random.next_u32() as i32;

        let header: RequestHeaderV2 = RequestHeaderV2 {
            request_api_key: Request::get_api_key(),
            request_api_version: Request::get_version(),
            correlation_id,
            client_id: NullableString(Some(String::from(self.client_id))),
            tag_buffer: TaggedFields::new()
        };

        let pair: PairWithI32EncodedSize<RequestHeaderV2, Request> = PairWithI32EncodedSize(header, request);
        pair.to_kafka_bytes(&mut tcp_stream)?;
        let response_size: usize = u32::from_kafka_bytes(&mut tcp_stream).unwrap() as usize;

        let mut response_vec: Vec<u8> = vec![0u8; response_size];
        tcp_stream.read(response_vec.as_mut_slice()).unwrap();
        debug!("response_vec: {:?}", response_vec);
        let mut response_reader: Reader<&[u8]> = response_vec.reader();

        let response_correlation_id: i32 = deserialize_response_header_and_get_correlation_id(
            &mut response_reader, Request::get_response_header_version()
        )?;

        if response_correlation_id != correlation_id {
            return Err(
                anyhow!("Correlation id mismatch. Request correlation id was {} and response correlation id was {}",
                        correlation_id, response_correlation_id)
            );
        }

        let response = Response::from_kafka_bytes(&mut response_reader)?;
        trace!("response: {:?}", response);
        Ok(response)
    }
}


// fn test_request_and_response<Request: KafkaRequest, Respon>() {
//
// }

#[test]
fn test_networking_stuff_with_struct() {
    let mut networking_client: KafkaNetworkingClient = KafkaNetworkingClient::new("127.0.0.1:9092", "rusty");
    let request: ApiVersionsRequestV3 = ApiVersionsRequestV3 {
        client_software_name: CompactString(String::from("kafkart")),
        client_software_version: CompactString(String::from("0.0.1")),
        tag_buffer: TaggedFields {
            tags: VarArray(Vec::new())
        }
    };
    let response: ApiVersionsResponseV3 = networking_client.send_request_and_get_response(request).unwrap();
    debug!("response: {:?}", response);
}
