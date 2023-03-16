use std::iter::Map;
use proc_macro2::{Ident, TokenStream};
use quote::{TokenStreamExt, ToTokens};
use quote::__private::ext::RepToTokensExt;
use syn;
use syn::{Attribute, Data, Field, Fields, GenericArgument, parse_macro_input, PathArguments, PathSegment, Type};
use syn::DeriveInput;
use syn::punctuated::{Iter, Pair, Punctuated};
use syn::token::Colon2;
use anyhow::{anyhow, Result};

#[cfg(test)]
mod tests;

/// Provides an implementation of `KafkaEncodable` for structs which are not one of the
/// protocol primitives defined here: https://kafka.apache.org/protocol.html#protocol_types
///
/// # Examples
/// TODO!
#[proc_macro_derive(KafkaEncodable, attributes(kafka_encodable_tagged_fields))]
pub fn derive_kafka_encodable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let generated_impl: proc_macro2::TokenStream = generate_kafka_encodable_impl(parse_macro_input!(input)).unwrap();
    generated_impl.into()
}

fn generate_kafka_encodable_impl(syntax_tree: DeriveInput) -> Result<proc_macro2::TokenStream> {
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
                    if has_kafka_encodable_tagged_fields_attr {
                        generate_kafka_encodable_impl_for_tagged_fields(struct_name, fields)
                    } else {
                        generate_kafka_encodable_impl_for_struct(struct_name, fields)
                    }
                },
                _ => panic!("Only structs with named fields are supported")
            }
        },
        _ => panic!("Only structs are supported")
    }
}

fn generate_kafka_encodable_impl_for_struct(struct_name: &Ident, fields: Vec<Field>) -> Result<proc_macro2::TokenStream> {
    let field_names: Vec<Ident> = fields.into_iter()
        .map(|field| field.ident.expect("Field did not have an ident"))
        .collect();
    Ok(quote::quote! {
        impl KafkaEncodable for #struct_name {
            #[tracing::instrument]
            fn to_kafka_bytes<W: std::io::Write + std::fmt::Debug>(self, writer: &mut W) -> anyhow::Result<()> {
                #(
                    self.#field_names.to_kafka_bytes(writer)?;
                )*
                Ok(())
            }
        }
    })
}

fn generate_kafka_encodable_impl_for_tagged_fields(struct_name: &Ident, fields: Vec<Field>) -> Result<proc_macro2::TokenStream> {
    let field_names: Vec<Ident> = fields.into_iter()
        .map(|field| field.ident.expect("Field did not have an ident"))
        .collect();

    let mut field_lines: Vec<proc_macro2::TokenStream> = Vec::new();
    for i in 0..field_names.len() as u32 {
        let current_field: &Ident = field_names.get(i as usize).ok_or(anyhow!("lol"))?;
        field_lines.push(quote::quote! {
            match self.#current_field {
                Some(field) => {
                    let tag: UnsignedVarInt32 = UnsignedVarInt32(#i);
                    let mut buffer: Vec<u8> = Vec::new();
                    field.to_kafka_bytes(&mut buffer)?;
                    let size: UnsignedVarInt32 = UnsignedVarInt32(buffer.len() as u32);

                    tag.to_kafka_bytes(writer)?;
                    size.to_kafka_bytes(writer)?;
                    buffer.to_kafka_bytes(writer)?;
                },
                None => {}
            };
        });
    }
    Ok(quote::quote! {
        impl KafkaEncodable for #struct_name {
            #[tracing::instrument]
            fn to_kafka_bytes<W: std::io::Write + std::fmt::Debug>(self, writer: &mut W) -> anyhow::Result<()> {
                let mut num_tagged_fields: i32 = 0;
                #(
                    match self.#field_names {
                        Some(_) => num_tagged_fields += 1,
                        None => {}
                    };
                )*
                num_tagged_fields.to_kafka_bytes(writer)?;

                #(
                    #field_lines
                )*
                Ok(())
            }
        }
    })
}

fn get_path_segment_from_type(t: Type) -> Result<PathSegment> {
    match t {
        Type::Path(type_path) => {
            let segments: Punctuated<PathSegment, Colon2> = type_path.path.segments;
            if segments.len() != 1usize {
                return Err(anyhow!("A field had {:?} path segments in its type when only one was expected", segments.len()));
            }
            return segments.into_iter().next().ok_or(anyhow!("Could not access the type segment in a field"));
        },
        any_other_type => {
            return Err(anyhow!("Expected type to be of type path but was instead {:?}", any_other_type));
        }
    }
}

fn generate_kafka_decodable_impl_for_tagged_fields(struct_name: &Ident, fields: Vec<Field>) -> Result<proc_macro2::TokenStream> {
    let mut field_tags: Vec<u32> = (0..fields.len() as u32).collect();

    let mut field_idents: Vec<Ident> = Vec::new();
    let mut field_types: Vec<Type> = Vec::new();
    for field in fields {
        let ident = field.ident.ok_or(anyhow!("A field with type {:?} is missing an identifier", field.ty))?;
        field_idents.push(ident);
        field_types.push(field.ty);
    }

    let mut inner_field_types: Vec<TokenStream> = Vec::new();
    let mut inner_field_types_without_generics: Vec<Ident> = Vec::new();

    for field_type in field_types {
        let segment: PathSegment = get_path_segment_from_type(field_type)?;

        let path_arguments: PathArguments = segment.arguments;
        match path_arguments {
            PathArguments::AngleBracketed(angle_bracketed_arguments) => {
                let generic_arguments = angle_bracketed_arguments.args;
                if generic_arguments.len() != 1usize {
                    return Err(anyhow!("A field had {:?} generic arguments in its type when only one was expected", generic_arguments.len()));
                }
                let generic_argument: GenericArgument = generic_arguments.into_iter().next()
                    .ok_or(anyhow!("Could not access the angle-bracketed generic argument in a field type"))?;
                println!("generic_argument: {:?}", generic_argument);

                inner_field_types.push(generic_argument.to_token_stream());
                match generic_argument {
                    GenericArgument::Type(t) => {
                        let segment: PathSegment = get_path_segment_from_type(t)?;
                        inner_field_types_without_generics.push(segment.ident);
                    }
                    any_other_generic_argument=> {
                        return Err(anyhow!("Expected a field with a generic type argument, but was instead {:?}", any_other_generic_argument));
                    }
                }
            }
            any_other_arguments => {
                return Err(anyhow!("Expected a field with a type segment with angle bracketed path arguments, but was instead {:?}", any_other_arguments));
            }
        };
    }

    Ok(quote::quote! {
        impl KafkaDecodable for #struct_name {
            #[tracing::instrument]
            fn from_kafka_bytes<R: std::io::Read + std::fmt::Debug>(reader: &mut R) -> anyhow::Result<#struct_name> {
                let num_tagged_fields: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;

                #(
                    let mut #field_idents: Option<#inner_field_types> = None;
                )*

                for i in 0..num_tagged_fields.0 {
                    let tag: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;

                    match tag.0 {
                        #(
                            #field_tags => {
                                match #field_idents {
                                    Some(existing_field) => return Err(anyhow::anyhow!("tagged field was already set! {:?}", existing_field)),
                                    None => {
                                        let size: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
                                        #field_idents = Some(#inner_field_types_without_generics::from_kafka_bytes(reader)?);
                                    }
                                };
                            },
                        )*
                        unsupported_feature => {
                            // read and throw it away
                            kafka_encode::primitives::VarArray::<u8>::from_kafka_bytes(reader)?;
                        }
                    };
                }

                return Ok(
                    #struct_name {
                        #(
                            #field_idents,
                        )*
                    }
                );
            }
        }
    })
}

fn generate_kafka_decodable_impl_for_struct(struct_name: &Ident, struct_initializer_lines: Vec<proc_macro2::TokenStream>) -> Result<proc_macro2::TokenStream> {
    Ok(quote::quote! {
        impl KafkaDecodable for #struct_name {
            #[tracing::instrument]
            fn from_kafka_bytes<R: std::io::Read + std::fmt::Debug>(reader: &mut R) -> anyhow::Result<#struct_name> {
                let s: #struct_name = #struct_name {
                    #(
                        #struct_initializer_lines
                    )*
                };
                Ok(s)
            }
        }
    })
}

#[proc_macro_derive(KafkaDecodable, attributes(kafka_decodable_tagged_fields))]
pub fn derive_kafka_decodable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let generated_impl: proc_macro2::TokenStream = generate_kafka_decodable_impl(parse_macro_input!(input)).unwrap();
    generated_impl.into()
}

fn generate_kafka_decodable_impl(syntax_tree: DeriveInput) -> Result<proc_macro2::TokenStream> {
    // panic!("{:?}", syntax_tree);
    let struct_name: &Ident = &syntax_tree.ident;

    let attributes: Vec<Attribute> = syntax_tree.attrs;
    let has_kafka_decodable_tagged_fields_attr: bool = attributes.iter().any(
        |attribute: &Attribute| attribute.path.segments.pairs()
            .any(|pair: Pair<&PathSegment, &Colon2>| pair.value().ident.to_string() == "kafka_decodable_tagged_fields")
    );

    match syntax_tree.data {
        Data::Struct(data_struct) => {
            match data_struct.fields {
                Fields::Named(fields_named) => {
                    let fields: Vec<Field> = fields_named.named.into_iter().collect();

                    if has_kafka_decodable_tagged_fields_attr {
                        generate_kafka_decodable_impl_for_tagged_fields(struct_name, fields)
                    } else {
                        let struct_initializer_lines: Vec<proc_macro2::TokenStream> = generate_lines_in_struct_initializer(fields);
                        generate_kafka_decodable_impl_for_struct(struct_name, struct_initializer_lines)
                    }
                }
                _ => panic!("Only structs with named fields are supported")
            }
        },
        _ => panic!("Only structs are supported")
    }
}

fn generate_lines_in_struct_initializer(fields: Vec<Field>) -> Vec<proc_macro2::TokenStream> {
    let mut generated_lines: Vec<proc_macro2::TokenStream> = Vec::new();

    for field in fields {
        let field_ident: Ident = field.ident.unwrap();

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

fn generate_tokens_for_field_type_part(field_type_part: Pair<&PathSegment, &Colon2>) -> Result<proc_macro2::TokenStream> {
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
    Ok(token_stream)
}

// #[proc_macro_attribute]
// pub fn kafka_encodable_tagged_fields(_attribute: proc_macro::TokenStream, target_struct: proc_macro::TokenStream) -> proc_macro::TokenStream {
//     target_struct
// }