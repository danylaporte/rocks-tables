use serde::{Deserialize, Serialize};
use std::borrow::Cow;

pub trait AdaptToDb<'a> {
    type Schema: Serialize + for<'de> Deserialize<'de>;

    fn from_db(schema: Self::Schema) -> Self;
    fn to_db(&'a self) -> Self::Schema;
}

impl<'a, A, B> AdaptToDb<'a> for (A, B)
where
    A: AdaptToDb<'a>,
    B: AdaptToDb<'a>,
{
    type Schema = (A::Schema, B::Schema);

    fn from_db(schema: Self::Schema) -> Self {
        (A::from_db(schema.0), B::from_db(schema.1))
    }

    fn to_db(&'a self) -> Self::Schema {
        (self.0.to_db(), self.1.to_db())
    }
}

macro_rules! adapt_to_db {
    ($t:ty) => {
        impl<'a> AdaptToDb<'a> for $t {
            type Schema = $t;

            fn from_db(schema: Self::Schema) -> Self {
                schema
            }

            fn to_db(&'a self) -> Self::Schema {
                *self
            }
        }
    };
}

adapt_to_db!(i8);
adapt_to_db!(i16);
adapt_to_db!(i32);
adapt_to_db!(i64);
adapt_to_db!(u8);
adapt_to_db!(u16);
adapt_to_db!(u32);
adapt_to_db!(u64);

#[cfg(feature = "chrono")]
adapt_to_db!(chrono::DateTime<chrono::Utc>);

#[cfg(feature = "chrono")]
adapt_to_db!(chrono::NaiveDate);

#[cfg(feature = "chrono")]
adapt_to_db!(chrono::NaiveDateTime);

#[cfg(feature = "uuid")]
adapt_to_db!(uuid::Uuid);

impl<'a> AdaptToDb<'a> for String {
    type Schema = String;

    fn from_db(schema: Self::Schema) -> Self {
        schema
    }

    fn to_db(&'a self) -> Self::Schema {
        self.clone()
    }
}

impl<'a, T> AdaptToDb<'a> for Cow<'a, T>
where
    T: Clone + for<'de> Deserialize<'de> + Serialize,
{
    type Schema = T;

    fn from_db(schema: Self::Schema) -> Self {
        Cow::Owned(schema)
    }

    fn to_db(&'a self) -> Self::Schema {
        match self {
            Cow::Borrowed(b) => *b,
            Cow::Owned(b) => b,
        }
        .clone()
    }
}
