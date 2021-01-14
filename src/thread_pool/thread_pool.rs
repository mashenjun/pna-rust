use crate::thread_pool::ThreadPool;
use crate::Result;

pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> {
        return Ok(Self {});
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        unimplemented!()
    }
}

pub struct RayonThreadPool {}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        return Ok(Self {});
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        unimplemented!()
    }
}

pub struct SharedQueueThreadPool {}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        return Ok(Self {});
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        unimplemented!()
    }
}
