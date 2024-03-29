use bytes::{Bytes, BytesMut};
use proc_macro2::{Ident, TokenStream};
use syn::{Data, DeriveInput, Field, Fields, Item, PathArguments, PathSegment, Type};
use std::io::{Read, Result};
use quote::{TokenStreamExt, ToTokens};
use syn::__private::TokenStream2;
use syn::punctuated::Iter;
use syn::token::Colon2;
use kafka_encode_derive::KafkaEncodable;
use kafka_encode::KafkaEncodable;
use crate::protocol::api_versions::{ApiVersionsRequestV3, ApiVersionsResponseV3};
use crate::protocol::headers::RequestHeaderV2;


