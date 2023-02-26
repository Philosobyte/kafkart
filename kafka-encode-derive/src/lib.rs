use std::iter::Map;
use proc_macro2::Ident;
use quote::{TokenStreamExt, ToTokens};
use syn;
use syn::{Data, Field, Fields, parse_macro_input, PathArguments, PathSegment, Type};
use syn::DeriveInput;
use syn::punctuated::{Iter, Pair};
use syn::token::Colon2;

#[cfg(test)]
mod tests;

/// Provides an implementation of `KafkaEncodable` for structs which are not one of the
/// protocol primitives defined here: https://kafka.apache.org/protocol.html#protocol_types
///
/// # Examples
/// TODO!
#[proc_macro_derive(KafkaEncodable)]
pub fn derive_kafka_encodable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let generated_impl: proc_macro2::TokenStream = generate_kafka_encodable_impl(parse_macro_input!(input));
    generated_impl.into()
}

fn generate_kafka_encodable_impl(syntax_tree: DeriveInput) -> proc_macro2::TokenStream {
    let struct_name: &Ident = &syntax_tree.ident;

    match syntax_tree.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields_named) => {
                    generate_kafka_encodable_impl_for_struct(struct_name, fields_named.named.iter())
                },
                _ => panic!("Only structs with named fields are supported")
            }
        },
        _ => panic!("Only structs are supported")
    }
}

fn generate_kafka_encodable_impl_for_struct(struct_name: &Ident, fields: Iter<Field>) -> proc_macro2::TokenStream {
    let field_names = fields.map(|field| field.ident.as_ref().expect("Unable to determine field name"));
    quote::quote! {
        impl KafkaEncodable for #struct_name {
            fn to_kafka_bytes<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
                #(
                    self.#field_names.to_kafka_bytes(writer)?;
                )*
                Ok(())
            }
        }
    }
}

fn generate_kafka_decodable_impl_for_struct(struct_name: &Ident, struct_initializer_lines: Vec<proc_macro2::TokenStream>) -> proc_macro2::TokenStream {
    quote::quote! {
        impl KafkaDecodable for #struct_name {
            fn from_kafka_bytes<R: std::io::Read>(reader: &mut R) -> std::io::Result<#struct_name> {
                let s: #struct_name = #struct_name {
                    #(
                        #struct_initializer_lines
                    )*
                };
                Ok(s)
            }
        }
    }
}

#[proc_macro_derive(KafkaDecodable)]
pub fn derive_kafka_decodable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let generated_impl: proc_macro2::TokenStream = generate_kafka_decodable_impl(parse_macro_input!(input));
    generated_impl.into()
}

fn generate_kafka_decodable_impl(syntax_tree: DeriveInput) -> proc_macro2::TokenStream {
    let struct_name: &Ident = &syntax_tree.ident;
    match syntax_tree.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields_named) => {
                    let fields: Iter<Field> = fields_named.named.iter();
                    let struct_initializer_lines: Vec<proc_macro2::TokenStream> = generate_lines_in_struct_initializer(fields);
                    generate_kafka_decodable_impl_for_struct(struct_name, struct_initializer_lines)
                }
                _ => panic!("Only structs with named fields are supported")
            }
        },
        _ => panic!("Only structs are supported")
    }
}

fn generate_lines_in_struct_initializer(fields: Iter<Field>) -> Vec<proc_macro2::TokenStream> {
    let mut generated_lines: Vec<proc_macro2::TokenStream> = Vec::new();

    for field in fields {
        let field_ident: Ident = field.ident.as_ref().unwrap().clone();

        let field_type_prefix: proc_macro2::TokenStream = match &field.ty {
            Type::Path(type_path) => {
                let mut token_accumulator: proc_macro2::TokenStream = proc_macro2::TokenStream::new();
                for segment_and_colon2 in type_path.path.segments.pairs() {
                    token_accumulator.extend(generate_tokens_for_field_type_part(segment_and_colon2));
                }
                token_accumulator
            }
            _ => panic!("Only fields defined as paths are supported")
        };

        let generated_line: proc_macro2::TokenStream = quote::quote!(#field_ident: #field_type_prefix::from_kafka_bytes(reader)?,);
        generated_lines.push(generated_line);
    }
    generated_lines
}

fn generate_tokens_for_field_type_part(field_type_part: Pair<&PathSegment, &Colon2>) -> proc_macro2::TokenStream {
    let segment: &&PathSegment = field_type_part.value();
    let colon2_option: Option<&&Colon2> = field_type_part.punct();

    let mut token_stream: proc_macro2::TokenStream = proc_macro2::TokenStream::new();

    let segment_ident: Ident = segment.ident.clone();
    // name of the segment is first
    token_stream.append(segment_ident);

    // then any generics
    match &segment.arguments {
        PathArguments::None => {},
        PathArguments::AngleBracketed(angle_bracketed) => {
            let angle_bracketed_tokens: proc_macro2::TokenStream = angle_bracketed.to_token_stream();
            // TODO: use append instead of extend
            token_stream.extend(Colon2::default().to_token_stream());
            token_stream.extend(angle_bracketed_tokens);
        }
        _ => panic!("Only field types with angle bracketed generics or no generics are supported")
    }

    // then the double colon
    match colon2_option {
        None => {},
        Some(colon2) => {
            // TODO: use append instead of extend
            token_stream.extend(colon2.to_token_stream());
        }
    }
    token_stream
}
