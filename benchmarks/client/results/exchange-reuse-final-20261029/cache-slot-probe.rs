use crossbeam_utils::atomic::AtomicCell;
use std::pin::Pin;
fn main() {
    type Storage = Option<Pin<Box<[u8; 8000]>>>;
    println!("storage_bytes={} lock_free={}", std::mem::size_of::<Storage>(), AtomicCell::<Storage>::is_lock_free());
    assert!(AtomicCell::<Storage>::is_lock_free());
}
