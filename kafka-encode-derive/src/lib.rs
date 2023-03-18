use std::iter::Map;
use proc_macro2::{Ident, TokenStream};
use quote::{quote, TokenStreamExt, ToTokens};
use quote::__private::ext::RepToTokensExt;
use syn;
use syn::{Attribute, Data, Field, Fields, GenericArgument, parse_macro_input, PathArguments, PathSegment, Type};
use syn::DeriveInput;
use syn::punctuated::{Iter, Pair, Punctuated};
use syn::token::Colon2;
use anyhow::{anyhow, Result};
use crate::from_kafka_bytes_impl::{generate_from_kafka_bytes_impl, generate_from_kafka_bytes_impl_for_tagged_fields};
use crate::to_kafka_bytes_impl::{generate_to_kafka_bytes_impl, generate_to_kafka_bytes_impl_for_tagged_fields};

#[cfg(test)]
mod tests;
mod from_kafka_bytes_impl;
mod to_kafka_bytes_impl;

/// Provides an implementation of `KafkaEncodable` for structs which are not one of the
/// protocol primitives defined here: https://kafka.apache.org/protocol.html#protocol_types
///
/// # Examples
/// TODO!
#[proc_macro_derive(KafkaEncodable, attributes(kafka_encodable_tagged_fields))]
pub fn derive_kafka_encodable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let generated_impl: TokenStream = generate_kafka_encodable_impl(parse_macro_input!(input)).unwrap();
    generated_impl.into()
}

fn generate_kafka_encodable_impl(syntax_tree: DeriveInput) -> Result<TokenStream> {
    let struct_name: &Ident = &syntax_tree.ident;

    let attributes: Vec<Attribute> = syntax_tree.attrs;
    let has_kafka_encodable_tagged_fields_attr: bool = attributes.iter().any(
        |attribute: &Attribute| attribute.path.segments.pairs()
            .any(|pair: Pair<&PathSegment, &Colon2>| pair.value().ident.to_string() == "kafka_encodable_tagged_fields")
    );
    dbg!(has_kafka_encodable_tagged_fields_attr);
    match syntax_tree.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields_named) => {
                    let fields: Vec<Field> = fields_named.named.into_iter().collect();
                    let generated_to_kafka_bytes_impl = if has_kafka_encodable_tagged_fields_attr {
                        generate_to_kafka_bytes_impl_for_tagged_fields(struct_name, fields.clone())?
                    } else {
                        generate_to_kafka_bytes_impl(struct_name, fields.clone())?
                    };

                    let generated_from_kafka_bytes_impl = if has_kafka_encodable_tagged_fields_attr {
                        generate_from_kafka_bytes_impl_for_tagged_fields(struct_name, fields)?
                    } else {
                        generate_from_kafka_bytes_impl(struct_name, fields)?
                    };

                    return Ok(quote! {
                        impl KafkaEncodable for #struct_name {
                            #generated_to_kafka_bytes_impl

                            #generated_from_kafka_bytes_impl
                        }
                    });
                },
                _ => return Err(anyhow!("Only structs with named fields are supported"))
            };
        },
        _ => return Err(anyhow!("Only structs are supported"))
    };
}
