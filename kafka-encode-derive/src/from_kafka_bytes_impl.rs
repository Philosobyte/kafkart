use anyhow::{Result, anyhow};
use proc_macro2::{Ident, TokenStream};
use quote::{TokenStreamExt, ToTokens};
use syn::{Field, GenericArgument, PathArguments, PathSegment, Type};
use syn::punctuated::{Pair, Punctuated};
use syn::token::{Colon2, Comma};

/// Generates an implementation for the `from_kafka_bytes` function in the `KafkaEncodable` trait
/// for structs with named fields.
pub(crate) fn generate_from_kafka_bytes_impl(struct_name: &Ident, fields: Vec<Field>) -> Result<TokenStream> {
    let (field_idents, field_types) = split_fields_into_idents_and_types(fields)?;

    // these are for accessing the `from_kafka_bytes` associated function in fields
    let mut field_paths: Vec<TokenStream> = Vec::with_capacity(field_types.len());
    for field_type in field_types {
        field_paths.push(build_path_from_type(field_type)?);
    }

    Ok(quote::quote! {
        #[tracing::instrument]
        fn from_kafka_bytes<R: std::io::Read + std::fmt::Debug>(reader: &mut R) -> anyhow::Result<#struct_name> {
            let s: #struct_name = #struct_name {
                #(
                    #field_idents: #field_paths::from_kafka_bytes(reader)?,
                )*
            };
            Ok(s)
        }
    })
}

/// Generates an implementation for the `from_kafka_bytes` function in the `KafkaEncodable` trait
/// for structs whose primary purpose is to hold tagged fields. Because no tagged fields are required
/// by the protocol, each field in such a struct must be wrapped in an `Option`.
pub(crate) fn generate_from_kafka_bytes_impl_for_tagged_fields(struct_name: &Ident, fields: Vec<Field>) -> Result<TokenStream> {
    // we'll match on tag numbers to identify which field is being deserialized
    let mut field_tags: Vec<u32> = (0..fields.len() as u32).collect();

    let (field_idents, field_types) = split_fields_into_idents_and_types(fields)?;
    let (inner_field_types, inner_field_type_paths) = extract_inner_types_and_paths_from_option_field(field_types)?;

    Ok(quote::quote! {
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
                                Some(existing_field) => return Err(anyhow::anyhow!("Visiting a tagged field which has already been set! {:?}", existing_field)),
                                None => {
                                    let size: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
                                    #field_idents = Some(#inner_field_type_paths::from_kafka_bytes(reader)?);
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
    })
}

/// Take Option field types, such as `Option<String>` or `Option<Vec<u8>>`, and convert them into
/// 1. inner field types, e.g. `String` and `Vec<u8>`, and
/// 2. inner field paths, e.g. `String` and `Vec::<u8>`
fn extract_inner_types_and_paths_from_option_field(field_types: Vec<Type>) -> Result<(Vec<TokenStream>, Vec<TokenStream>)> {
    // inner field type
    let mut inner_field_types: Vec<TokenStream> = Vec::new();
    let mut inner_field_type_paths: Vec<TokenStream> = Vec::new();

    for field_type in field_types {
        // every field must be an Option
        let option_segment: PathSegment = get_single_path_segment_from_type(field_type)?;

        let option_arguments: PathArguments = option_segment.arguments;
        match option_arguments {
            PathArguments::AngleBracketed(angle_bracketed_arguments) => {
                let generic_arguments: Punctuated<GenericArgument, Comma> = angle_bracketed_arguments.args;
                if generic_arguments.len() != 1usize {
                    return Err(anyhow!("A field had {:?} generic arguments in its type, but only one is expected", generic_arguments.len()));
                }
                let generic_argument: GenericArgument = generic_arguments.into_iter().next()
                    .ok_or(anyhow!("Could not access the angle-bracketed generic argument in a field type"))?;

                inner_field_types.push(generic_argument.to_token_stream());
                match generic_argument {
                    GenericArgument::Type(t) => {
                        let path = build_path_from_type(t)?;
                        inner_field_type_paths.push(path);
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

    return Ok((inner_field_types, inner_field_type_paths));
}

fn get_single_path_segment_from_type(t: Type) -> Result<PathSegment> {
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

fn split_fields_into_idents_and_types(fields: Vec<Field>) -> Result<(Vec<Ident>, Vec<Type>)> {
    let mut field_idents: Vec<Ident> = Vec::with_capacity(fields.len());
    let mut field_types: Vec<Type> = Vec::with_capacity(fields.len());

    for field in fields {
        field_idents.push(field.ident.ok_or(anyhow!("A field is missing ident"))?);
        field_types.push(field.ty);
    }
    return Ok((field_idents, field_types));
}

/// Take a type, like `kafka_encode::primitives::CompactArray<u8>`, and build a path from it,
/// like `kafka_encode::primitives::CompactArray::<u8>`
fn build_path_from_type(ty: Type) -> Result<TokenStream> {
    let mut built_path: TokenStream = TokenStream::new();

    let path_segments: Punctuated<PathSegment, Colon2> = match ty {
        Type::Path(type_path) => {
            Ok(type_path.path.segments)
        },
        any_other_type => {
            Err(anyhow!("Expected type to be a path but was instead {:?}", any_other_type))
        }
    }?;

    for segment in path_segments {
        built_path.append_separated(segment.ident.to_token_stream(), Colon2::default());
        match segment.arguments {
            PathArguments::None => {},
            PathArguments::AngleBracketed(angle_bracketed) => {
                built_path.append_all(Colon2::default().to_token_stream());
                built_path.append_all(angle_bracketed.to_token_stream());
            },
            _ => {
                return Err(anyhow!("Expected either angle bracketed generics or no arguments at all"));
            }
        }
    }
    return Ok(built_path);
}
