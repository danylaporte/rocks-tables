use crate::{Aged, Db, Result, UpdateFrom};
use serde::{Deserialize, Serialize};
use std::{
    collections::hash_map::{HashMap, RandomState},
    fmt::Debug,
    hash::{BuildHasher, Hash},
};

/// A table that keep in memory only a small percent of the real table.
/// Last recent used items are discard from memory when the capacity is reached.
pub struct LruTable<K, V, S = RandomState> {
    age: u64,
    db: Db<K>,
    map: HashMap<K, Aged<V>, S>,
}

impl<K, V> LruTable<K, V, RandomState>
where
    K: Debug + for<'de> Deserialize<'de> + Eq + Hash + Serialize,
    V: for<'de> Deserialize<'de> + Serialize,
{
    /// Creates a LruCachedTable.
    pub fn with_capacity(db: Db<K>, capacity: usize) -> Self {
        Self::with_capacity_and_hasher(db, capacity, Default::default())
    }
}

impl<K, V, S> LruTable<K, V, S>
where
    K: Debug + for<'de> Deserialize<'de> + Eq + Hash + Serialize,
    V: for<'de> Deserialize<'de> + Serialize,
    S: BuildHasher,
{
    /// Creates a LruCachedTable with a hasher.
    pub fn with_capacity_and_hasher(db: Db<K>, capacity: usize, hash_builder: S) -> Self {
        assert!(capacity > 0);

        Self {
            age: 0,
            db,
            map: HashMap::with_capacity_and_hasher(capacity, hash_builder),
        }
    }

    /// Returns true if the table contains a value for the specified key.
    pub fn contains_key(&self, key: &K) -> Result<bool> {
        Ok(if self.map.contains_key(key) {
            true
        } else {
            self.db.contains_key(key)?
        })
    }

    /// Removes a key from the table.
    pub fn delete(&mut self, key: &K) -> Result<()> {
        self.db.delete(key)?;
        self.map.remove(key);
        Ok(())
    }

    fn ensure_capacity(&mut self)
    where
        K: Clone,
    {
        if self.map.capacity() == self.map.len() {
            if let Some(key) = self.map.iter().min_by_key(|t| t.1.age).map(|t| t.0.clone()) {
                self.map.remove(&key);
            }
        }
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get(&mut self, key: &K) -> Result<Option<&V>>
    where
        K: Clone,
    {
        if !self.map.contains_key(key) {
            self.ensure_capacity();

            match self.db.get(key)? {
                Some(value) => {
                    let value = value.to_inner()?;
                    self.map.insert(key.clone(), Aged { age: 0, value });
                }
                None => return Ok(None),
            }
        }

        let age = &mut self.age;

        Ok(self.map.get_mut(key).map(|v| {
            *age += 1;
            v.age = *age;
            &v.value
        }))
    }

    pub fn put(&mut self, key: &K, value: V) -> Result<()>
    where
        K: Clone,
    {
        self.db.put(key, &value)?;
        self.age += 1;

        let age = self.age;

        match self.map.get_mut(key) {
            Some(aged) => {
                aged.age = age;
                aged.value = value;
            }
            None => {
                self.ensure_capacity();
                self.map.insert(key.clone(), Aged { age, value });
            }
        }

        Ok(())
    }

    pub fn update<U>(&mut self, key: &K, update: U) -> Result<()>
    where
        K: Clone,
        U: UpdateFrom<V>,
    {
        let must_ensure_capacity;

        let mut new = match self.map.remove(key) {
            Some(mut aged) => {
                must_ensure_capacity = false;
                aged.value = update.update_from(Some(aged.value));
                aged
            }
            None => {
                must_ensure_capacity = true;

                let mut old = None;

                if let Some(item) = self.db.get(key)? {
                    old = Some(item.to_inner()?);
                }

                Aged {
                    age: 0,
                    value: update.update_from(old),
                }
            }
        };

        self.db.put(key, &new.value)?;

        if must_ensure_capacity {
            self.ensure_capacity();
        }

        self.age += 1;
        new.age = self.age;

        self.map.insert(key.clone(), new);
        Ok(())
    }

    pub fn update_and_get<U>(&mut self, key: &K, update: U) -> Result<&mut V>
    where
        K: Clone,
        U: UpdateFrom<V>,
    {
        self.update(key, update)?;
        Ok(&mut self.map.get_mut(key).unwrap().value)
    }
}
