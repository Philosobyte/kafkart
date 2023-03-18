use anyhow::{Result, anyhow};
use proc_macro2::Ident;
use syn::Field;

/// Generates an implementation for the `to_kafka_bytes` function in the `KafkaEncodable` trait
/// for structs with named fields.
pub(crate) fn generate_to_kafka_bytes_impl(struct_name: &Ident, fields: Vec<Field>) -> Result<proc_macro2::TokenStream> {
    let field_names: Vec<Ident> = fields.into_iter()
        .map(|field| field.ident.expect("Field did not have an ident"))
        .collect();
    Ok(quote::quote! {
        #[tracing::instrument]
        fn to_kafka_bytes<W: std::io::Write + std::fmt::Debug>(self, writer: &mut W) -> anyhow::Result<()> {
            #(
                self.#field_names.to_kafka_bytes(writer)?;
            )*
            Ok(())
        }
    })
}

/// Generates an implementation for the `to_kafka_bytes` function in the `KafkaEncodable` trait
/// for structs whose primary purpose is to hold tagged fields. Because no tagged fields are required
/// by the protocol, each field in such a struct must be wrapped in an `Option`.
pub(crate) fn generate_to_kafka_bytes_impl_for_tagged_fields(struct_name: &Ident, fields: Vec<Field>) -> Result<proc_macro2::TokenStream> {
    let field_names: Vec<Ident> = fields.into_iter()
        .map(|field| field.ident.expect("Field did not have an ident"))
        .collect();

    let mut lines_to_serialize_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    for i in 0..field_names.len() as u32 {
        let current_field: &Ident = field_names.get(i as usize).ok_or(anyhow!("Could not access a field's name"))?;

        lines_to_serialize_fields.push(quote::quote! {
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

    return Ok(quote::quote! {
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
                #lines_to_serialize_fields
            )*
            Ok(())
        }
    })}
