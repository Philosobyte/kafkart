use std::fmt::Debug;
use syn::{DeriveInput, File};
use crate::{generate_kafka_encodable_impl, generate_kafka_decodable_impl};

#[test]
fn test_derive_kafka_encodable() {
    let input: proc_macro2::TokenStream = quote::quote! {
        pub struct StructWithNamedFields {
            pub string: String,
            pub integer: i32,
            pub byte_vec: Vec<u8>,
            pub string_vec: Vec<String>,
            pub compact_string_array: CompactArray<String>
        }
    };

    let abstract_syntax_tree: DeriveInput = syn::parse2(input).unwrap();
    let output: proc_macro2::TokenStream = generate_kafka_encodable_impl(abstract_syntax_tree);

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
        }
    };
    assert_eq!(output.to_string(), expected_output.to_string());
}

#[test]
fn test_derive_kafka_decodable() {
    let input: proc_macro2::TokenStream = quote::quote! {
        pub struct StructWithNamedFields {
            pub string: String,
            pub integer: i32,
            pub byte_vec: Vec<u8>,
            pub string_vec: Vec<String>,
            pub compact_string_array: CompactArray<String>
        }
    };

    let abstract_syntax_tree: DeriveInput = syn::parse2(input).unwrap();
    let output: proc_macro2::TokenStream = generate_kafka_decodable_impl(abstract_syntax_tree);

    let expected_output: proc_macro2::TokenStream = quote::quote! {
        impl KafkaDecodable for StructWithNamedFields {
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
    assert_eq!(output.to_string(), expected_output.to_string());
}
