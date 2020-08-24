use crate::{deserialize_from_bytes, serialize_to_bytes, Encrypt, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

enum Data<'a> {
    Owned(Vec<u8>),
    Ref(&'a [u8]),
}

impl<'a> Data<'a> {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Owned(v) => v,
            Self::Ref(v) => v,
        }
    }

    pub fn as_ref(&self) -> Data {
        Data::Ref(self.as_bytes())
    }

    fn to_owned(&self) -> Data<'static> {
        Data::Owned(self.as_bytes().to_vec())
    }
}

impl<'a, 'de: 'a> Deserialize<'de> for Data<'a> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::Ref(Deserialize::deserialize(deserializer)?))
    }
}

impl<'a> Serialize for Data<'a> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_bytes().serialize(serializer)
    }
}

/// Delay the deserialization of the binary type.
///
/// By keeping the value as serialized, we can minimize the copy of the data.
pub struct Binary<'a>(Data<'a>);

impl<'a> Binary<'a> {
    #[inline]
    pub fn with_ref<T>(value: &T) -> Result<Self>
    where
        T: Serialize,
    {
        Ok(Self(Data::Owned(serialize_to_bytes(value)?)))
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn as_ref(&self) -> Binary {
        Binary(self.0.as_ref())
    }

    #[inline]
    pub fn to_inner<'de, T>(&'de self) -> Result<T>
    where
        T: Deserialize<'de>,
    {
        deserialize_from_bytes(self.0.as_bytes())
    }
}

impl<'a> AsRef<[u8]> for Binary<'a> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a, 'de: 'a> Deserialize<'de> for Binary<'a> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(Deserialize::deserialize(deserializer)?))
    }
}

impl<'a> Serialize for Binary<'a> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

pub struct Crypted<'a>(Data<'a>);

impl<'a> Crypted<'a> {
    pub fn with_ref<T, E>(value: &T, nonce: &[u8], cypher: &E) -> Result<Crypted<'static>>
    where
        E: Encrypt,
        T: Serialize,
    {
        let bytes = serialize_to_bytes(value)?;
        let bytes = cypher.encrypt(&bytes, nonce)?;
        Ok(Crypted(Data::Owned(bytes)))
    }

    pub fn as_ref(&self) -> Crypted {
        Crypted(self.0.as_ref())
    }

    pub fn to_inner<'de, T, E>(&self, nonce: &[u8], cypher: &E, temp: &'de mut Vec<u8>) -> Result<T>
    where
        E: Encrypt,
        T: Deserialize<'de>,
    {
        let data = self.0.as_bytes();
        *temp = cypher.decrypt(data, nonce)?;
        deserialize_from_bytes(temp)
    }

    pub fn to_owned(&self) -> Crypted<'static> {
        Crypted(self.0.to_owned())
    }
}

impl<'a, 'de: 'a> Deserialize<'de> for Crypted<'a> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self(Data::deserialize(deserializer)?))
    }
}

impl<'a> Serialize for Crypted<'a> {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}
