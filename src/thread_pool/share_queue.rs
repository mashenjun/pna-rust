use crate::thread_pool::ThreadPool;
use crate::Result;
use crossbeam::channel::{bounded, Receiver, Sender};
use std::thread;

type CanRun = Box<dyn FnOnce() + Send + 'static>;

pub struct SharedQueueThreadPool {
    sender: Sender<CanRun>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (sender, receiver) = bounded::<CanRun>((threads * 3) as usize);
        for _ in 0..threads {
            // spawn thread and let thread to loop on the receiver
            let rx = receiver.clone();
            thread::Builder::new().spawn(move || execute(rx))?;
        }
        Ok(Self { sender })
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Err(e) = self.sender.send(Box::new(f)) {
            error!("send msg err: {}", e);
        }
    }
}

fn execute(rx: Receiver<CanRun>) {
    // Option1: check thread::panicking() in some deconstruct or defer point, if the current thread
    // is in panicking state, spawn a new thread and let the current on exit.
    // Option2: use catch_unwind to catch the panic, and continue use the current thread.

    defer! {
       if thread::panicking() {
       let rx = rx.clone();
       if let Err(e) = thread::Builder::new().spawn(move || execute(rx)) {
                error!("recover thread err: {}", e);
            }
        }
    }
    loop {
        match rx.recv() {
            Ok(run) => {
                // the run() may panic
                run();
                // if let Err(_) = panic::catch_unwind(AssertUnwindSafe(|| {
                //     run();
                // })) {
                //     warn!("catch panic");
                // }
            }
            Err(_) => return info!("exit thread"),
        }
    }
}
