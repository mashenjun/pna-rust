pub use share_queue::SharedQueueThreadPool;
pub use thread_pool::{NaiveThreadPool, RayonThreadPool};

mod share_queue;
mod thread_pool;

use crate::Result;

pub trait ThreadPool: Sized {
    fn new(threads: u32) -> Result<Self>;
    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static;
}
