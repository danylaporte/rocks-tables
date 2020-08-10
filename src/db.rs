use crate::{AdaptToDb, Error, Result};
use aes_gcm::{
    aead::{generic_array::GenericArray, Aead},
    Aes256Gcm,
};
use bincode::{
    config::{BigEndian, WithOtherEndian},
    Options as _,
};
pub use rocksdb::Direction;
use rocksdb::{DBCompressionType, DBRawIterator, Options};
use std::{marker::PhantomData, path::Path};

pub struct Db<'a, K, V> {
    _k: PhantomData<K>,
    _v: PhantomData<V>,
    bin_opts: BinOpts,
    db: rocksdb::DB,
    encrypt: Option<&'a Aes256Gcm>,
}

impl<K, V> Db<'static, K, V> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut o = Options::default();
        o.create_if_missing(true);
        o.set_compression_type(DBCompressionType::Zstd);

        Ok(Db {
            _k: PhantomData,
            _v: PhantomData,
            bin_opts: bin_opts(),
            db: rocksdb::DB::open(&o, path)?,
            encrypt: None,
        })
    }
}

impl<'a, K, V> Db<'a, K, V>
where
    for<'b> K: AdaptToDb<'b>,
    for<'b> V: AdaptToDb<'b>,
{
    pub fn with_encrypt<P: AsRef<Path>>(path: P, aes_gcm: &'a Aes256Gcm) -> Result<Self> {
        let mut db = Db::new(path)?;
        db.encrypt = Some(aes_gcm);
        Ok(db)
    }

    pub fn contains_key(&self, key: &K) -> Result<bool> {
        Ok(self.get_raw(key)?.is_some())
    }

    pub fn delete(&self, key: &K) -> Result<()> {
        let key = self.bin_opts.serialize(&key.to_db())?;
        self.db.delete(&key)?;
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        Ok(match self.get_raw(key)? {
            Some(bytes) => Some(V::from_db(self.bin_opts.deserialize(&bytes)?)),
            None => None,
        })
    }

    fn get_raw(&self, key: &K) -> Result<Option<Vec<u8>>> {
        let key = self.bin_opts.serialize(&key.to_db())?;

        let value = match self.db.get(&key)? {
            Some(value) => value,
            None => return Ok(None),
        };

        Ok(Some(match self.encrypt {
            Some(cipher) => {
                let mut fallback = [0u8; 12];
                let nonce = prepare_nonce(&key, &mut fallback);
                cipher.decrypt(nonce, &value[..])?
            }
            None => value,
        }))
    }

    pub fn iter<'b>(&'b self, mode: IteratorMode<K>) -> Result<Iter<'b, K, V>> {
        let bin_opts = &self.bin_opts;
        let mut iter = self.db.raw_iterator();

        let dir = match mode {
            IteratorMode::From(v, dir) => {
                let key = bin_opts.serialize(&v.to_db())?;

                match dir {
                    Direction::Forward => iter.seek(&key),
                    Direction::Reverse => iter.seek_for_prev(&key),
                }

                dir
            }
            IteratorMode::End => {
                iter.seek_to_last();
                Direction::Reverse
            }
            IteratorMode::Start => {
                iter.seek_to_first();
                Direction::Forward
            }
        };

        Ok(Iter {
            _k: PhantomData,
            _v: PhantomData,
            bin_opts,
            dir,
            encrypt: self.encrypt,
            iter,
            must_call_next: false,
        })
    }

    pub fn put(&self, key: &K, value: &V) -> Result<()> {
        // serializing keys in big endian to preserve sorting order when iterating the db.
        let key = self.bin_opts.serialize(&key.to_db())?;
        let val = self.bin_opts.serialize(&value.to_db())?;

        match &self.encrypt {
            Some(cypher) => {
                let mut fallback = [0u8; 12];
                let nonce = prepare_nonce(&key, &mut fallback);
                let encrypted = cypher.encrypt(nonce, &val[..])?;

                Ok(self.db.put(&key, encrypted)?)
            }
            None => Ok(self.db.put(&key, &val)?),
        }
    }
}

type BinOpts = WithOtherEndian<bincode::DefaultOptions, BigEndian>;

fn bin_opts() -> BinOpts {
    bincode::options().with_big_endian()
}

#[derive(Clone, Copy)]
pub struct DbKeyValue<'a, K, V> {
    _k: PhantomData<K>,
    _v: PhantomData<V>,
    bin_opts: &'a BinOpts,
    encrypt: Option<&'a Aes256Gcm>,
    iter: &'a DBRawIterator<'a>,
}

impl<'a, K, V> DbKeyValue<'a, K, V>
where
    K: for<'b> AdaptToDb<'b>,
{
    pub fn get_key(&self) -> Result<K> {
        Ok(K::from_db(self.bin_opts.deserialize(
            self.iter.key().ok_or_else(|| Error::NoKey)?,
        )?))
    }

    pub fn into_db_value(self) -> DbValue<'a, V> {
        DbValue {
            _v: PhantomData,
            bin_opts: self.bin_opts,
            encrypt: self.encrypt,
            iter: self.iter,
        }
    }

    pub fn into_key_and_db_value(self) -> Result<(K, DbValue<'a, V>)> {
        Ok((
            self.get_key()?,
            DbValue {
                _v: PhantomData,
                bin_opts: self.bin_opts,
                encrypt: self.encrypt,
                iter: self.iter,
            },
        ))
    }
}

pub struct DbValue<'a, V> {
    _v: PhantomData<V>,
    bin_opts: &'a BinOpts,
    encrypt: Option<&'a Aes256Gcm>,
    iter: &'a DBRawIterator<'a>,
}

impl<'a, V> DbValue<'a, V>
where
    V: AdaptToDb<'a>,
{
    pub fn into_value(self) -> Result<V> {
        let value = self.iter.value().ok_or_else(|| Error::NoValue)?;
        let decrypted;

        let value = match self.encrypt {
            None => value,
            Some(cipher) => {
                let key = self.iter.key().ok_or_else(|| Error::NoKey)?;
                let mut fallback = [0u8; 12];
                let nonce = prepare_nonce(&key, &mut fallback);

                decrypted = cipher.decrypt(nonce, &value[..])?;

                &decrypted
            }
        };

        Ok(V::from_db(self.bin_opts.deserialize(value)?))
    }
}

pub enum IteratorMode<K> {
    End,
    From(K, Direction),
    Start,
}

pub struct Iter<'a, K, V> {
    _k: PhantomData<K>,
    _v: PhantomData<V>,
    bin_opts: &'a BinOpts,
    dir: Direction,
    encrypt: Option<&'a Aes256Gcm>,
    iter: DBRawIterator<'a>,
    must_call_next: bool,
}

impl<'a, K, V> Iter<'a, K, V> {
    pub fn current(&self) -> DbKeyValue<K, V> {
        DbKeyValue {
            _k: PhantomData,
            _v: PhantomData,
            bin_opts: self.bin_opts,
            encrypt: self.encrypt,
            iter: &self.iter,
        }
    }

    pub fn next(&mut self) -> Result<Option<DbKeyValue<K, V>>> {
        if self.must_call_next {
            match self.dir {
                Direction::Forward => self.iter.next(),
                Direction::Reverse => self.iter.prev(),
            }

            self.iter.status()?;
        }

        self.must_call_next = true;

        Ok(if self.iter.valid() {
            Some(self.current())
        } else {
            None
        })
    }
}

fn prepare_nonce<'a>(
    key: &'a [u8],
    fallback: &'a mut [u8; 12],
) -> &'a GenericArray<u8, aes_gcm::aead::consts::U12> {
    // aes needs nonce of 12 bytes.
    if key.len() >= 12 {
        // if the key is longer than the required len, we just take the required data from the key.
        // This is a zero copy (fastest)
        GenericArray::from_slice(&key[0..12])
    } else {
        // if the key is shorter than the required len, we pad with 0
        // This requires copy but since we are using a fallback, we do not need allocation (faster).
        *fallback = [0u8; 12];
        fallback.copy_from_slice(&key);
        GenericArray::from_slice(&*fallback)
    }
}
