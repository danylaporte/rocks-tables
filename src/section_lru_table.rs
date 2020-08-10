use super::{AdaptToDb, Aged, Db, IteratorMode, MinValue, Result};
use rocksdb::Direction;
use std::{
    collections::{hash_map::RandomState, HashMap},
    hash::{BuildHasher, Hash},
};

/// A tables that keep section of records in memory and remove the last recently used section.
pub struct SectionLruTable<'a, S, K, V, H = RandomState> {
    age: u64,
    db: Db<'a, (S, K), V>,
    map: HashMap<S, Aged<HashMap<K, V, H>>, H>,
}

impl<'a, S, K, V> SectionLruTable<'a, S, K, V, RandomState>
where
    S: for<'b> AdaptToDb<'b> + Clone + Eq + Hash,
    K: for<'b> AdaptToDb<'b> + Eq + Hash + MinValue,
    V: for<'b> AdaptToDb<'b>,
{
    pub fn with_capacity(db: Db<'a, (S, K), V>, capacity: usize) -> Self {
        Self::with_capacity_and_hasher(db, capacity, Default::default())
    }
}

impl<'a, S, K, V, H> SectionLruTable<'a, S, K, V, H>
where
    S: for<'b> AdaptToDb<'b> + Clone + Eq + Hash,
    K: for<'b> AdaptToDb<'b> + Eq + Hash + MinValue,
    V: for<'b> AdaptToDb<'b>,
    H: BuildHasher + Default,
{
    pub fn with_capacity_and_hasher(db: Db<'a, (S, K), V>, capacity: usize, hasher: H) -> Self {
        assert!(capacity > 0);

        Self {
            age: 0,
            db,
            map: HashMap::with_capacity_and_hasher(capacity, hasher),
        }
    }

    pub fn contains_key(&self, section: S, key: &K) -> Result<bool>
    where
        K: Clone,
    {
        match self.map.get(&section) {
            Some(section) => Ok(section.value.contains_key(key)),
            None => self.db.contains_key(&(section, key.clone())),
        }
    }

    pub fn delete(&mut self, section: S, key: &K) -> Result<()>
    where
        K: Clone,
    {
        match self.map.get_mut(&section) {
            Some(aged) => {
                if aged.value.contains_key(key) {
                    self.db.delete(&(section.clone(), key.clone()))?;
                    aged.value.remove(key);
                }
            }
            None => self.db.delete(&(section, key.clone()))?,
        }

        Ok(())
    }

    fn ensure_capacity(&mut self) {
        if self.map.capacity() == self.map.len() {
            if let Some(key) = self.map.iter().min_by_key(|t| t.1.age).map(|t| t.0.clone()) {
                self.map.remove(&key);
            }
        }
    }

    fn ensure_section_loaded(&mut self, section: S) -> Result<&mut HashMap<K, V, H>> {
        self.age += 1;

        if !self.map.contains_key(&section) {
            self.ensure_capacity();

            self.map.insert(
                section.clone(),
                Aged {
                    age: 0,
                    value: load_map(section.clone(), &self.db)?,
                },
            );
        }

        let aged = self.map.get_mut(&section).unwrap();
        aged.age = self.age;
        Ok(&mut aged.value)
    }

    pub fn get(&mut self, section: S, key: &K) -> Result<Option<&V>> {
        Ok(self.ensure_section_loaded(section)?.get(key))
    }

    pub fn get_section(&mut self, section: S) -> Result<&HashMap<K, V, H>> {
        Ok(&*self.ensure_section_loaded(section)?)
    }

    pub fn put(&mut self, section: S, key: &K, value: V) -> Result<()>
    where
        K: Clone,
    {
        self.db.put(&(section.clone(), key.clone()), &value)?;

        self.ensure_section_loaded(section)?
            .insert(key.clone(), value);
        Ok(())
    }
}

fn load_map<K, V, S, H>(section: S, db: &Db<(S, K), V>) -> Result<HashMap<K, V, H>>
where
    K: for<'a> AdaptToDb<'a> + Eq + Hash + MinValue,
    V: for<'a> AdaptToDb<'a>,
    S: for<'a> AdaptToDb<'a> + Clone + PartialEq,
    H: BuildHasher + Default,
{
    let key = (section.clone(), K::min_value());
    let mode = IteratorMode::From(key, Direction::Forward);
    let mut iter = db.iter(mode)?;
    let mut map = HashMap::with_hasher(Default::default());

    while let Some(item) = iter.next()? {
        let ((s, key), db_value) = item.into_key_and_db_value()?;

        if s != section {
            break;
        }

        let value = db_value.into_value()?;
        map.insert(key, value);
    }

    Ok(map)
}
