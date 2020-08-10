pub trait UpdateFrom<T> {
    fn update_from(self, old: Option<T>) -> T;
}
