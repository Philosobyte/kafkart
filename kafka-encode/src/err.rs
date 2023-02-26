use std::fmt::{Display, Formatter};

/// Some types in Kafka's network protocol are limited in size, and this error indicates those limits are being exceeded
#[derive(Debug)]
pub struct TooManyBytesError {
    pub size: usize
}

impl Display for TooManyBytesError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bytes of size {} was too large", &self.size)
    }
}

impl std::error::Error for TooManyBytesError {
}
