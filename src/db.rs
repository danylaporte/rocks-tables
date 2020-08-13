use crate::{AdaptToDb, Error, Result};
use aes_gcm::{
    aead::{generic_array::GenericArray, Aead},
    Aes256Gcm,
};
use bincode::{
    config::{BigEndian, WithOtherEndian},
    Options as _,
};
use fmt::Display;
pub use rocksdb::Direction;
use rocksdb::{DBCompressionType, DBRawIterator, Options};
use std::{
    fmt::{self, Debug},
    marker::PhantomData,
    path::Path,
    sync::Arc,
};
use tracing::{error, trace_span, Span};

pub struct Db<'a, K, V> {
    _k: PhantomData<K>,
    _v: PhantomData<V>,
    bin_opts: BinOpts,
    db: rocksdb::DB,
    db_name: String,
    encrypt: Option<&'a Aes256Gcm>,
}

impl<K, V> Db<'static, K, V> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_name = path
            .as_ref()
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

        let span = trace_span!(
            "Db::open",
            db.name = db_name.as_str(),
            db.system = "rocksdb",
        );
        let _ = span.enter();

        let mut o = Options::default();
        o.create_if_missing(true);
        o.set_compression_type(DBCompressionType::Zstd);

        Ok(Db {
            _k: PhantomData,
            _v: PhantomData,
            bin_opts: bin_opts(),
            db: rocksdb::DB::open(&o, path).map_err(span_err)?,
            db_name,
            encrypt: None,
        })
    }
}

impl<'a, K, V> Db<'a, K, V>
where
    for<'b> K: AdaptToDb<'b> + Debug,
    for<'b> V: AdaptToDb<'b>,
{
    pub fn open_encrypted<P: AsRef<Path>>(path: P, aes_gcm: &'a Aes256Gcm) -> Result<Self> {
        let mut db = Db::open(path)?;
        db.encrypt = Some(aes_gcm);
        Ok(db)
    }

    pub fn contains_key(&self, key: &K) -> Result<bool> {
        let span = trace_span!(
            "Db::contains_key",
            db.name = self.db_name.as_str(),
            db.statement = format!("{:?}", key).as_str(),
            db.system = "rocksdb",
            http.status_code = "200",
        );
        let _ = span.enter();

        Ok(self.get_raw(key).map_err(span_err)?.is_some())
    }

    pub fn delete(&self, key: &K) -> Result<()> {
        let span = trace_span!(
            "Db::delete",
            db.name = self.db_name.as_str(),
            db.statement = format!("{:?}", key).as_str(),
            db.system = "rocksdb",
            http.status_code = "200",
        );
        let _ = span.enter();

        let key = self.bin_opts.serialize(&key.to_db()).map_err(span_err)?;
        self.db.delete(&key).map_err(span_err)?;
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let span = trace_span!(
            "Db::get",
            db.name = self.db_name.as_str(),
            db.statement = format!("{:?}", key).as_str(),
            db.system = "rocksdb",
            http.status_code = "200",
        );
        let _ = span.enter();

        Ok(match self.get_raw(key)? {
            Some(bytes) => Some(V::from_db(
                self.bin_opts.deserialize(&bytes).map_err(span_err)?,
            )),
            None => None,
        })
    }

    fn get_raw(&self, key: &K) -> Result<Option<Vec<u8>>> {
        let key = self.bin_opts.serialize(&key.to_db()).map_err(span_err)?;

        let value = match self.db.get(&key).map_err(span_err)? {
            Some(value) => value,
            None => return Ok(None),
        };

        Ok(Some(match self.encrypt {
            Some(cipher) => {
                let mut fallback = [0u8; 12];
                let nonce = prepare_nonce(&key, &mut fallback);
                cipher.decrypt(nonce, &value[..]).map_err(span_err)?
            }
            None => value,
        }))
    }

    pub fn iter(&self, mode: IteratorMode<K>) -> Result<Iter<K, V>> {
        let span = Arc::new(trace_span!(
            "Db::iter",
            db.name = self.db_name.as_str(),
            db.statement = format!("mode = {:?}", mode).as_str(),
            db.system = "rocksdb",
            http.status_code = "200",
        ));
        let _ = span.enter();

        let bin_opts = &self.bin_opts;
        let mut iter = self.db.raw_iterator();

        let dir = match mode {
            IteratorMode::From(v, dir) => {
                let key = bin_opts.serialize(&v.to_db()).map_err(span_err)?;

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
            db_name: &self.db_name,
            encrypt: self.encrypt,
            iter,
            must_call_next: false,
        })
    }

    pub fn put(&self, key: &K, value: &V) -> Result<()> {
        let span = trace_span!(
            "Db::put",
            db.name = self.db_name.as_str(),
            db.statement = format!("{:?}", key).as_str(),
            db.system = "rocksdb",
            http.status_code = "200",
        );
        let _ = span.enter();

        // serializing keys in big endian to preserve sorting order when iterating the db.
        let key = self.bin_opts.serialize(&key.to_db()).map_err(span_err)?;
        let val = self.bin_opts.serialize(&value.to_db()).map_err(span_err)?;

        match &self.encrypt {
            Some(cypher) => {
                let mut fallback = [0u8; 12];
                let nonce = prepare_nonce(&key, &mut fallback);
                let encrypted = cypher.encrypt(nonce, &val[..]).map_err(span_err)?;

                Ok(self.db.put(&key, encrypted).map_err(span_err)?)
            }
            None => Ok(self.db.put(&key, &val).map_err(span_err)?),
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
    db_name: &'a str,
    encrypt: Option<&'a Aes256Gcm>,
    iter: &'a DBRawIterator<'a>,
}

impl<'a, K, V> DbKeyValue<'a, K, V>
where
    K: for<'b> AdaptToDb<'b>,
{
    pub fn get_key(&self) -> Result<K> {
        Ok(K::from_db(
            self.bin_opts.deserialize(
                self.iter
                    .key()
                    .ok_or_else(|| Error::NoKey)
                    .map_err(|e| log_err(self.db_name, "get_key", e))?,
            )?,
        ))
    }

    pub fn into_db_value(self) -> DbValue<'a, V> {
        DbValue {
            _v: PhantomData,
            bin_opts: self.bin_opts,
            db_name: self.db_name,
            encrypt: self.encrypt,
            iter: self.iter,
        }
    }

    pub fn into_key_and_db_value(self) -> Result<(K, DbValue<'a, V>)> {
        Ok((
            self.get_key()
                .map_err(|e| log_err(self.db_name, "into_key_and_db_value", e))?,
            DbValue {
                _v: PhantomData,
                bin_opts: self.bin_opts,
                db_name: self.db_name,
                encrypt: self.encrypt,
                iter: self.iter,
            },
        ))
    }
}

pub struct DbValue<'a, V> {
    _v: PhantomData<V>,
    bin_opts: &'a BinOpts,
    db_name: &'a str,
    encrypt: Option<&'a Aes256Gcm>,
    iter: &'a DBRawIterator<'a>,
}

impl<'a, V> DbValue<'a, V>
where
    V: AdaptToDb<'a>,
{
    pub fn into_value(self) -> Result<V> {
        let value = self
            .iter
            .value()
            .ok_or_else(|| Error::NoValue)
            .map_err(|e| log_err(self.db_name, "into_value", e))?;

        let decrypted;

        let value = match self.encrypt {
            None => value,
            Some(cipher) => {
                let key = self
                    .iter
                    .key()
                    .ok_or_else(|| Error::NoKey)
                    .map_err(|e| log_err(self.db_name, "into_value", e))?;

                let mut fallback = [0u8; 12];
                let nonce = prepare_nonce(&key, &mut fallback);

                decrypted = cipher
                    .decrypt(nonce, &value[..])
                    .map_err(|e| log_err(self.db_name, "into_value", e))?;

                &decrypted
            }
        };

        Ok(V::from_db(
            self.bin_opts
                .deserialize(value)
                .map_err(|e| log_err(self.db_name, "into_value", e))?,
        ))
    }
}

pub enum IteratorMode<K> {
    End,
    From(K, Direction),
    Start,
}

impl<K> Debug for IteratorMode<K>
where
    K: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IteratorMode::")?;
        match self {
            Self::End => f.write_str("End"),
            Self::From(key, dir) => {
                f.write_str("From(")?;
                key.fmt(f)?;
                f.write_str(",")?;
                f.write_str(match dir {
                    Direction::Forward => "Forward)",
                    Direction::Reverse => "Reverse)",
                })
            }
            Self::Start => f.write_str("Start"),
        }
    }
}

pub struct Iter<'a, K, V> {
    _k: PhantomData<K>,
    _v: PhantomData<V>,
    bin_opts: &'a BinOpts,
    db_name: &'a str,
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
            db_name: self.db_name,
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

            self.iter
                .status()
                .map_err(|e| log_err(self.db_name, "next", e))?;
        }

        self.must_call_next = true;

        Ok(if self.iter.valid() {
            Some(self.current())
        } else {
            None
        })
    }
}

fn log_err<E: Display>(db_name: &str, db_operation: &str, e: E) -> E {
    error!({ db.name = db_name, db.operation = db_operation, db.system = "rocksdb" }, "{}", e);
    e
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

fn span_err<E: Display>(e: E) -> E {
    Span::current().record("http.status_code", &"500");
    error!("{}", e);
    e
}
