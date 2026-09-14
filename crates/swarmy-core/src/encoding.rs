//! Stored values are a one-byte version followed by a postcard payload.
//!
//! Version 1 uses Serde's external enum tags (numeric discriminants in postcard).
//! New variants must be appended so old tags retain their meaning. Changing field
//! order or types requires a new storage version and a decoder for the old schema.
//! New readers can read old variants; old readers reject unknown variants.

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

pub const STORAGE_VERSION: u8 = 1;

#[derive(Debug, Error)]
pub enum EncodingError {
    #[error("stored value is missing its version byte")]
    MissingVersion,
    #[error("unsupported stored value version {0}; supported version is {STORAGE_VERSION}")]
    UnknownVersion(u8),
    #[error("invalid postcard payload: {0}")]
    Payload(#[from] postcard::Error),
    #[error("stored value has {0} trailing bytes")]
    TrailingBytes(usize),
}

/// Encode a stored value using the current version.
///
/// # Errors
/// Returns an error if the value cannot be serialized by postcard.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, EncodingError> {
    Ok(postcard::to_extend(value, vec![STORAGE_VERSION])?)
}

/// Decode exactly one stored value after checking its version.
///
/// # Errors
/// Rejects missing or unknown versions, malformed payloads, and trailing bytes.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, EncodingError> {
    let (&version, payload) = bytes.split_first().ok_or(EncodingError::MissingVersion)?;
    if version != STORAGE_VERSION {
        return Err(EncodingError::UnknownVersion(version));
    }
    let (value, remainder) = postcard::take_from_bytes(payload)?;
    if !remainder.is_empty() {
        return Err(EncodingError::TrailingBytes(remainder.len()));
    }
    Ok(value)
}

/// JSON values need their own type information, which postcard does not provide.
/// Store only these open-ended fields as JSON strings inside the binary payload;
/// human-readable serializers retain ordinary JSON objects, arrays, and scalars.
pub(crate) mod json {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};

    pub fn serialize<T: Serialize, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            value.serialize(serializer)
        } else {
            serde_json::to_string(value)
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer)
        }
    }

    pub fn deserialize<'de, T: DeserializeOwned, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        if deserializer.is_human_readable() {
            T::deserialize(deserializer)
        } else {
            let json = String::deserialize(deserializer)?;
            serde_json::from_str(&json).map_err(serde::de::Error::custom)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::fmt::Debug;

    pub(crate) fn assert_round_trip<T: Serialize + DeserializeOwned + PartialEq + Debug>(
        value: &T,
    ) {
        let bytes = encode(value).unwrap();
        assert_eq!(bytes[0], STORAGE_VERSION);
        assert_eq!(&decode::<T>(&bytes).unwrap(), value);
        let json = serde_json::to_string(value).unwrap();
        assert_eq!(&serde_json::from_str::<T>(&json).unwrap(), value);
    }

    #[test]
    fn unknown_versions_are_rejected_before_reading_payloads() {
        for version in [0, 2, u8::MAX] {
            let mut bytes = encode(&42_u64).unwrap();
            bytes[0] = version;
            let error = decode::<u64>(&bytes).unwrap_err();
            assert!(matches!(error, EncodingError::UnknownVersion(found) if found == version));
            assert_eq!(
                error.to_string(),
                format!("unsupported stored value version {version}; supported version is 1")
            );
            assert!(matches!(
                decode::<u64>(&[version]),
                Err(EncodingError::UnknownVersion(_))
            ));
        }
    }

    #[test]
    fn malformed_values_are_rejected() {
        assert!(matches!(
            decode::<u64>(&[]),
            Err(EncodingError::MissingVersion)
        ));
        assert!(matches!(
            decode::<u64>(&[STORAGE_VERSION]),
            Err(EncodingError::Payload(_))
        ));
        assert!(matches!(
            decode::<bool>(&[STORAGE_VERSION, 2]),
            Err(EncodingError::Payload(_))
        ));
        let mut bytes = encode(&42_u64).unwrap();
        bytes.push(0);
        assert!(matches!(
            decode::<u64>(&bytes),
            Err(EncodingError::TrailingBytes(1))
        ));
    }

    #[test]
    fn version_one_has_a_single_byte_header() {
        assert_eq!(encode(&42_u64).unwrap(), [1, 42]);
        assert_eq!(decode::<u64>(&[1, 42]).unwrap(), 42);
    }
}
