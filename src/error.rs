use std::{error, fmt};

#[derive(Debug)]
pub enum Error {
    #[cfg(feature = "aes-gcm")]
    AesGcm(aes_gcm::Error),
    NoKey,
    NoValue,
    RocksDb(rocksdb::Error),
    Serde(Box<bincode::ErrorKind>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            #[cfg(feature = "aes-gcm")]
            Self::AesGcm(e) => {
                f.write_str("Encryption error: ")?;
                e.fmt(f)
            }
            Self::NoKey => f.write_str("No Key."),
            Self::NoValue => f.write_str("No Value."),
            Self::RocksDb(e) => {
                f.write_str("RocksDb error: ")?;
                e.fmt(f)
            }
            Self::Serde(e) => {
                f.write_str("Serialization error: ")?;
                e.fmt(f)
            }
        }
    }
}

impl error::Error for Error {}

#[cfg(feature = "aes-gcm")]
impl From<aes_gcm::Error> for Error {
    fn from(e: aes_gcm::Error) -> Self {
        Self::AesGcm(e)
    }
}

impl From<rocksdb::Error> for Error {
    fn from(e: rocksdb::Error) -> Self {
        Self::RocksDb(e)
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        Self::Serde(e)
    }
}
