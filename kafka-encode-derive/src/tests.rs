use std::fmt::Debug;
use syn::{DeriveInput, File};
use crate::generate_kafka_encodable_impl;

#[test]
fn test_derive_kafka_encodable() {
    let target_struct: proc_macro2::TokenStream = quote::quote! {
        pub struct StructWithNamedFields {
            pub string: String,
            pub integer: i32,
            pub byte_vec: Vec<u8>,
            pub string_vec: Vec<String>,
            pub compact_string_array: CompactArray<String>
        }
    };

    let abstract_syntax_tree: DeriveInput = syn::parse2(target_struct).unwrap();
    let generated_output: proc_macro2::TokenStream = generate_kafka_encodable_impl(abstract_syntax_tree).unwrap();

    let expected_output: proc_macro2::TokenStream = quote::quote! {
        impl KafkaEncodable for StructWithNamedFields {
            #[tracing::instrument]
            fn to_kafka_bytes<W: std::io::Write + std::fmt::Debug>(self, writer: &mut W) -> anyhow::Result<()> {
                self.string.to_kafka_bytes(writer)?;
                self.integer.to_kafka_bytes(writer)?;
                self.byte_vec.to_kafka_bytes(writer)?;
                self.string_vec.to_kafka_bytes(writer)?;
                self.compact_string_array.to_kafka_bytes(writer)?;
                Ok(())
            }

            #[tracing::instrument]
            fn from_kafka_bytes<R: std::io::Read + std::fmt::Debug>(reader: &mut R) -> anyhow::Result<StructWithNamedFields> {
                let s: StructWithNamedFields = StructWithNamedFields {
                    string: String::from_kafka_bytes(reader)?,
                    integer: i32::from_kafka_bytes(reader)?,
                    byte_vec: Vec::<u8>::from_kafka_bytes(reader)?,
                    string_vec: Vec::<String>::from_kafka_bytes(reader)?,
                    compact_string_array: CompactArray::<String>::from_kafka_bytes(reader)?,
                };
                Ok(s)
            }
        }
    };
    assert_eq!(generated_output.to_string(), expected_output.to_string());
}

#[test]
fn test_derive_kafka_encodable_for_tagged_fields() {
    let target_struct: proc_macro2::TokenStream = quote::quote! {
        #[kafka_encodable_tagged_fields]
        pub struct StructWithTags {
            pub string: Option<String>,
            pub integer: Option<i32>,
            pub compact_string_array: Option<CompactArray<String>>
        }
    };

    let abstract_syntax_tree: DeriveInput = syn::parse2(target_struct).unwrap();
    let output: proc_macro2::TokenStream = generate_kafka_encodable_impl(abstract_syntax_tree).unwrap();

    let expected_output: proc_macro2::TokenStream = quote::quote! {
        impl KafkaEncodable for StructWithTags {
            #[tracing::instrument]
            fn to_kafka_bytes<W: std::io::Write + std::fmt::Debug>(
                self,
                writer: &mut W
            ) -> anyhow::Result<()> {
                let mut num_tagged_fields: i32 = 0;
                match self.string {
                    Some(_) => num_tagged_fields += 1,
                    None => {}
                };
                match self.integer {
                    Some(_) => num_tagged_fields += 1,
                    None => {}
                };
                match self.compact_string_array {
                    Some(_) => num_tagged_fields += 1,
                    None => {}
                };
                num_tagged_fields.to_kafka_bytes(writer)?;
                match self.string {
                    Some(field) => {
                        let tag: UnsignedVarInt32 = UnsignedVarInt32(0u32);
                        let mut buffer: Vec<u8> = Vec::new();
                        field.to_kafka_bytes(&mut buffer)?;
                        let size: UnsignedVarInt32 = UnsignedVarInt32(buffer.len() as u32);
                        tag.to_kafka_bytes(writer)?;
                        size.to_kafka_bytes(writer)?;
                        buffer.to_kafka_bytes(writer)?;
                    },
                    None => {}
                };
                match self.integer {
                    Some(field) => {
                        let tag: UnsignedVarInt32 = UnsignedVarInt32(1u32);
                        let mut buffer: Vec<u8> = Vec::new();
                        field.to_kafka_bytes(&mut buffer)?;
                        let size: UnsignedVarInt32 = UnsignedVarInt32(buffer.len() as u32);
                        tag.to_kafka_bytes(writer)?;
                        size.to_kafka_bytes(writer)?;
                        buffer.to_kafka_bytes(writer)?;
                    },
                    None => {}
                };
                match self.compact_string_array {
                    Some(field) => {
                        let tag: UnsignedVarInt32 = UnsignedVarInt32(2u32);
                        let mut buffer: Vec<u8> = Vec::new();
                        field.to_kafka_bytes(&mut buffer)?;
                        let size: UnsignedVarInt32 = UnsignedVarInt32(buffer.len() as u32);
                        tag.to_kafka_bytes(writer)?;
                        size.to_kafka_bytes(writer)?;
                        buffer.to_kafka_bytes(writer)?;
                    },
                    None => {}
                };
                Ok(())
            }

            #[tracing::instrument]
            fn from_kafka_bytes<R: std::io::Read + std::fmt::Debug>(
                reader: &mut R
            ) -> anyhow::Result<StructWithTags> {
                let num_tagged_fields: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(
                    reader
                )?;
                let mut string: Option<String> = None;
                let mut integer: Option<i32> = None;
                let mut compact_string_array: Option<CompactArray<String> > = None;
                for i in 0..num_tagged_fields.0 {
                    let tag: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(reader)?;
                    match tag.0 {
                        0u32 => {
                            match string {
                                Some(existing_field) =>
                                    return Err(
                                        anyhow::anyhow!(
                                            "Visiting a tagged field which has already been set! {:?}", existing_field
                                        )
                                    ),
                                None => {
                                    let size: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(
                                        reader
                                    )?;
                                    string = Some(String::from_kafka_bytes(reader)?);
                                }
                            };
                        },
                        1u32 => {
                            match integer {
                                Some(existing_field) =>
                                    return Err(
                                        anyhow::anyhow!(
                                            "Visiting a tagged field which has already been set! {:?}", existing_field
                                        )
                                    ),
                                None => {
                                    let size: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(
                                        reader
                                    )?;
                                    integer = Some(i32::from_kafka_bytes(reader)?);
                                }
                            };
                        },
                        2u32 => {
                            match compact_string_array {
                                Some(existing_field) =>
                                    return Err(
                                        anyhow::anyhow!(
                                            "Visiting a tagged field which has already been set! {:?}", existing_field
                                        )
                                    ),
                                None => {
                                    let size: UnsignedVarInt32 = UnsignedVarInt32::from_kafka_bytes(
                                        reader
                                    )?;
                                    compact_string_array = Some(
                                        CompactArray::<String>::from_kafka_bytes(reader)?
                                    );
                                }
                            };
                        },
                        unsupported_feature => {
                            kafka_encode::primitives::VarArray::<u8>::from_kafka_bytes(reader)?;
                        }
                    };
                }
                return Ok(StructWithTags {
                    string,
                    integer,
                    compact_string_array,
                });
            }
        }
    };
    assert_eq!(output.to_string(), expected_output.to_string());
}
