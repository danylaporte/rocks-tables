use super::{AdaptToDb, Db, IteratorMode, Result};
use crate::UpdateFrom;
use std::{
    borrow::Borrow,
    collections::hash_map::{HashMap, Iter, RandomState},
    fmt::Debug,
    hash::{BuildHasher, Hash},
};

/// A fully in-memory loaded table.
pub struct MemTable<'a, K, V, S = RandomState> {
    db: Db<'a, K, V>,
    map: HashMap<K, V, S>,
}

impl<'a, K, V> MemTable<'a, K, V, RandomState>
where
    K: for<'b> AdaptToDb<'b> + Debug + Eq + Hash,
    V: for<'b> AdaptToDb<'b>,
{
    pub fn new(db: Db<'a, K, V>) -> Result<Self> {
        Self::with_hasher(db, Default::default())
    }
}

impl<'a, K, V, S> MemTable<'a, K, V, S>
where
    K: for<'b> AdaptToDb<'b> + Debug + Eq + Hash,
    V: for<'b> AdaptToDb<'b>,
    S: BuildHasher,
{
    pub fn with_hasher(db: Db<'a, K, V>, hasher: S) -> Result<Self> {
        let mut map = HashMap::with_hasher(hasher);

        {
            let mut iter = db.iter(IteratorMode::Start)?;

            while let Some(kv) = iter.next()? {
                let (key, db_value) = kv.into_key_and_db_value()?;
                let value = db_value.into_value()?;

                map.insert(key, value);
            }
        }

        Ok(Self { db, map })
    }

    /// Returns true if the table contains a value for the specified key.
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        Q: Eq + Hash,
        K: Borrow<Q>,
    {
        self.map.contains_key(key)
    }

    /// Removes a key from the table, returning the value at the key if the key was previously in the map.
    pub fn delete(&mut self, key: &K) -> Result<Option<V>> {
        Ok(if self.map.contains_key(key) {
            self.db.delete(key)?;
            self.map.remove(key)
        } else {
            None
        })
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.map.get(key)
    }

    pub fn iter(&self) -> Iter<K, V> {
        self.map.iter()
    }

    pub fn put(&mut self, key: &K, value: V) -> Result<()>
    where
        K: Clone,
    {
        self.db.put(&key, &value)?;

        match self.map.get_mut(key) {
            Some(v) => {
                *v = value;
            }
            None => {
                self.map.insert(key.clone(), value);
            }
        };

        Ok(())
    }

    pub fn update<U>(&mut self, key: K, update: U) -> Result<()>
    where
        U: UpdateFrom<V>,
    {
        let v = match self.map.remove(&key) {
            Some(old) => update.update_from(Some(old)),
            None => update.update_from(None),
        };

        let r = self.db.put(&key, &v);

        if r.is_err() {
            if let Some(v) = self.db.get(&key)? {
                self.map.insert(key, v);
            }
        } else {
            self.map.insert(key, v);
        }

        r
    }
}
