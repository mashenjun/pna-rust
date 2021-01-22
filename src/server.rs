use crate::thread_pool::ThreadPool;
use crate::{parse_request, KvsEngine, KvsError, Reply, Request, Result};
use nix::unistd::close;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::os::unix::io::AsRawFd;
use std::str;
use std::sync::atomic;
use std::time::Duration;

/// The server of a key value store.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
    socket: Socket,
    close: atomic::AtomicBool,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P) -> Result<Self> {
        let socket = Socket::new(Domain::ipv4(), Type::stream(), Some(Protocol::tcp()))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.set_linger(Some(Duration::new(0, 0)))?;
        Ok(KvsServer {
            engine,
            pool,
            socket,
            close: atomic::AtomicBool::new(false),
        })
    }

    pub fn run(&self, addr: SocketAddr) -> Result<()> {
        self.socket.bind(&SockAddr::from(addr))?;
        self.socket.listen(128)?;
        {
            loop {
                match self.socket.accept() {
                    Ok((s, _)) => {
                        let stream = s.into_tcp_stream();
                        let engine = self.engine.clone();
                        self.pool.spawn(move || {
                            if let Err(e) = handle(engine, stream) {
                                error!("handle failed: {}", e);
                            }
                        })
                    }
                    Err(e) => {
                        if self.close.load(atomic::Ordering::Relaxed) {
                            info!("closing server");
                            break;
                        }
                        error!("connection failed: {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        self.close.store(true, atomic::Ordering::Relaxed);
        if cfg!(target_os = "macos") {
            close(self.socket.as_raw_fd())?;
        } else {
            self.socket.shutdown(Shutdown::Both)?;
        }
        Ok(())
    }
}

fn handle<T: KvsEngine>(engine: T, stream: TcpStream) -> Result<()> {
    debug!("accept connection: {}", stream.peer_addr()?);
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let mut buffer = [0; 1024];
    loop {
        // reusable buffer
        let cnt = reader.read(&mut buffer)?;
        if cnt == 0 {
            return Ok(());
        }
        let data = str::from_utf8(buffer[..cnt].as_ref())?;
        let req = parse_request(data);
        match req {
            Err(_) => {
                return Err(KvsError::InvalidCommandError);
            }
            Ok((_, req)) => {
                match req {
                    Request::Get { key } => match engine.get(key) {
                        Ok(res) => {
                            if let Some(s) = res {
                                writer.write(Reply::SingleLine(s).to_resp().as_ref())?;
                            } else {
                                writer.write(
                                    Reply::SingleLine("Key not found".to_string())
                                        .to_resp()
                                        .as_ref(),
                                )?;
                            }
                            writer.flush()?;
                        }
                        Err(e) => {
                            writer.write(Reply::Err(e.to_string()).to_resp().as_ref())?;
                            writer.flush()?;
                        }
                    },

                    Request::Set { key, value } => match engine.set(key.clone(), value) {
                        Ok(_) => {
                            writer.write(Reply::SingleLine("".to_string()).to_resp().as_ref())?;
                            writer.flush()?;
                            // println!("done: {:?}", key);
                        }
                        Err(e) => {
                            writer.write(Reply::Err(e.to_string()).to_resp().as_ref())?;
                            writer.flush()?;
                        }
                    },
                    Request::Remove { key } => match engine.remove(key) {
                        Ok(_) => {
                            writer.write(Reply::SingleLine("".to_string()).to_resp().as_ref())?;
                            writer.flush()?;
                        }
                        Err(e) => {
                            writer.write(Reply::Err(e.to_string()).to_resp().as_ref())?;
                            writer.flush()?;
                        }
                    },
                }
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::unistd::close;
    use std::net::Shutdown;
    use std::os::unix::io::AsRawFd;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn shutdown_socket() {
        let socket =
            Arc::new(Socket::new(Domain::ipv4(), Type::stream(), Some(Protocol::tcp())).unwrap());
        socket
            .bind(&"127.0.0.1:4000".parse::<SocketAddr>().unwrap().into())
            .unwrap();
        socket.listen(128).unwrap();
        let ssocket = socket.clone();
        let handler = thread::spawn(move || match ssocket.accept() {
            Ok(_) => println!("new client"),
            Err(e) => println!("got error: {:?}", e),
        });

        std::thread::sleep(std::time::Duration::from_secs(1));
        if cfg!(target_os = "macos") {
            close(socket.as_raw_fd()).unwrap();
        } else {
            socket.shutdown(Shutdown::Both).unwrap();
        }
        handler.join().unwrap();
    }
}
