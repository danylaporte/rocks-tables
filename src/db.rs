use crate::{Error, Result};
use fmt::Display;
use rocksdb::{DBCompressionType, DBPinnableSlice, DBRawIterator, Options};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Debug},
    marker::PhantomData,
    path::Path,
    sync::Arc,
};
use tracing::{error, trace_span};

pub struct Db<K> {
    _k: PhantomData<K>,
    db: rocksdb::DB,
    db_name: String,
}

impl<K> Db<K>
where
    K: Debug + for<'de> Deserialize<'de> + Serialize,
{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_name = path
            .as_ref()
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

        let _ = trace_span!("open", db.name = db_name.as_str(), db.system = "rocksdb").enter();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(DBCompressionType::Zstd);

        Ok(Db {
            _k: PhantomData,
            db: rocksdb::DB::open(&opts, path).map_err(|e| map_log_err(e, &db_name))?,
            db_name,
        })
    }

    pub fn contains_key(&self, key: &K) -> Result<bool> {
        let _ = trace_span!(
            "contains_key",
            db.name = self.db_name.as_str(),
            db.statement = ?key,
            db.system = "rocksdb",
        )
        .enter();

        Ok(self.get_raw(key)?.is_some())
    }

    pub fn delete(&self, key: &K) -> Result<()> {
        let _ = trace_span!(
            "delete",
            db.name = self.db_name.as_str(),
            db.statement = ?key,
            db.system = "rocksdb",
        )
        .enter();

        let key = serialize_to_bytes(key, &self.db_name)?;

        self.db
            .delete(&key)
            .map_err(|e| map_log_err(e, &self.db_name))
    }

    /// Gets a value from the database.
    pub fn get(&self, key: &K) -> Result<Option<DbValue>> {
        let _ = trace_span!(
            "get",
            db.name = self.db_name.as_str(),
            db.statement = ?key,
            db.system = "rocksdb",
        )
        .enter();

        Ok(self.get_raw(key)?.map(|bytes| DbValue {
            bytes,
            db_name: &self.db_name,
        }))
    }

    fn get_raw<'a>(&'a self, key: &K) -> Result<Option<DBPinnableSlice<'a>>> {
        let key = serialize_to_bytes(key, &self.db_name)?;

        match self.db.get_pinned(&key) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(map_log_err(e, &self.db_name)),
        }
    }

    pub fn iter(&self, mode: IteratorMode<K>) -> Result<Iter<K>> {
        let span = Arc::new(trace_span!(
            "iter",
            db.name = self.db_name.as_str(),
            db.statement = format!("mode = {:?}", mode).as_str(),
            db.system = "rocksdb",
        ));
        let _ = span.enter();

        let mut iter = self.db.raw_iterator();

        let dir = match mode {
            IteratorMode::From(k, dir) => {
                let key = serialize_to_bytes(&k, &self.db_name)?;

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
            dir,
            db_name: &self.db_name,
            iter,
            must_call_next: false,
        })
    }

    pub fn put<V>(&self, key: &K, value: &V) -> Result<()>
    where
        V: Serialize,
    {
        let _ = trace_span!(
            "put",
            db.name = self.db_name.as_str(),
            db.statement = format!("{:?}", key).as_str(),
            db.system = "rocksdb",
        )
        .enter();

        let key = serialize_to_bytes(key, &self.db_name)?;
        let val = serialize_to_bytes(value, &self.db_name)?;

        self.db
            .put(&key, &val)
            .map_err(|e| map_log_err(e, &self.db_name))
    }
}

pub struct DbValue<'a> {
    bytes: DBPinnableSlice<'a>,
    db_name: &'a str,
}

impl<'a> DbValue<'a> {
    pub fn to_inner<'b, V>(&'b self) -> Result<V>
    where
        V: Deserialize<'b>,
    {
        deserialize_from_bytes(&self.bytes, self.db_name)
    }
}

pub struct DbKeyValue<'a, K> {
    _k: PhantomData<K>,
    db_name: &'a str,
    iter: &'a DBRawIterator<'a>,
}

impl<'a, K> DbKeyValue<'a, K> {
    pub fn key(&self) -> Result<K>
    where
        K: for<'de> Deserialize<'de>,
    {
        deserialize_from_bytes(self.key_as_bytes()?, self.db_name)
    }

    fn key_as_bytes(&self) -> Result<&[u8]> {
        self.iter
            .key()
            .ok_or_else(|| log_err(Error::NoKey, self.db_name))
    }

    pub fn value<'de, V>(&'de self) -> Result<V>
    where
        V: Deserialize<'de>,
    {
        deserialize_from_bytes(self.value_as_bytes()?, self.db_name)
    }

    fn value_as_bytes(&self) -> Result<&[u8]> {
        self.iter
            .value()
            .ok_or_else(|| log_err(Error::NoValue, self.db_name))
    }
}

#[derive(Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub enum Direction {
    Forward,
    Reverse,
}

impl From<Direction> for rocksdb::Direction {
    fn from(d: Direction) -> Self {
        match d {
            Direction::Forward => Self::Forward,
            Direction::Reverse => Self::Reverse,
        }
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

pub struct Iter<'a, K> {
    _k: PhantomData<K>,
    db_name: &'a str,
    dir: Direction,
    iter: DBRawIterator<'a>,
    must_call_next: bool,
}

impl<'a, K> Iter<'a, K> {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<DbKeyValue<K>>> {
        if self.must_call_next {
            match self.dir {
                Direction::Forward => self.iter.next(),
                Direction::Reverse => self.iter.prev(),
            }

            self.iter.status().map_err(|e| log_err(e, self.db_name))?;
        }

        self.must_call_next = true;

        Ok(if self.iter.valid() {
            Some(DbKeyValue {
                _k: PhantomData,
                db_name: self.db_name,
                iter: &self.iter,
            })
        } else {
            None
        })
    }
}

fn deserialize_from_bytes<'a, T: Deserialize<'a>>(bytes: &'a [u8], db_name: &str) -> Result<T> {
    crate::deserialize_from_bytes(bytes).map_err(|e| log_err(e, db_name))
}

fn log_err<E: Display>(e: E, db_name: &str) -> E {
    error!({ db.name = db_name, db.system = "rocksdb" }, "{}", e);
    e
}

fn map_log_err(e: rocksdb::Error, db_name: &str) -> Error {
    Error::RocksDb(log_err(e, db_name))
}

fn serialize_to_bytes<T: Serialize>(value: &T, db_name: &str) -> Result<Vec<u8>> {
    match crate::serialize_to_bytes(value) {
        Ok(o) => Ok(o),
        Err(e) => Err(log_err(e, db_name)),
    }
}
