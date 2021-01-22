use crate::thread_pool::ThreadPool;
use crate::Result;
use crossbeam::channel::{bounded, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;

type CanRun = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    Job(CanRun),
    Terminate,
}

pub struct SharedQueueThreadPool {
    sender: Sender<Message>,
    handles: Vec<JoinHandle<()>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let mut handles = vec![];
        let (sender, receiver) = bounded::<Message>((threads * 3) as usize);
        for _ in 0..threads {
            // spawn thread and let thread to loop on the receiver
            let rx = receiver.clone();
            let jh = thread::Builder::new().spawn(move || execute(rx))?;
            handles.push(jh)
        }
        Ok(Self { sender, handles })
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Err(e) = self.sender.send(Message::Job(Box::new(f))) {
            error!("send msg err: {}", e);
        }
    }
}

impl SharedQueueThreadPool {
    pub fn terminate(self) {
        for _ in &self.handles {
            self.sender
                .send(Message::Terminate)
                .expect("terminate thread with error");
        }

        for jh in self.handles {
            jh.join().unwrap();
        }
    }
}

fn execute(rx: Receiver<Message>) {
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
            Ok(msg) => match msg {
                // the run() may panic
                Message::Job(run) => {
                    run();
                }
                Message::Terminate => {
                    info!("terminate");
                    break;
                }
            },
            Err(_) => return info!("exit thread"),
        }
    }
}
