use crate::{Error, Result};
use bincode::{
    config::{BigEndian, WithOtherEndian},
    Options,
};
use serde::{Deserialize, Serialize};

fn bin_opts() -> WithOtherEndian<bincode::DefaultOptions, BigEndian> {
    // serializing keys in big endian to preserve sorting order when iterating the db.
    bincode::options().with_big_endian()
}

#[inline]
pub(super) fn deserialize_from_bytes<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    bin_opts().deserialize(bytes).map_err(Error::Serde)
}

#[inline]
pub(super) fn serialize_to_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bin_opts().serialize(value).map_err(Error::Serde)
}
