use crate::thread_pool::ThreadPool;
use crate::{parse_request, KvsEngine, KvsError, Reply, Request, Result};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str;

/// The server of a key value store.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }

    pub fn run(mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = handle(engine, stream) {
                        error!("handle failed: {}", e);
                    }
                }
                Err(e) => error!("connection failed: {}", e),
            })
        }
        Ok(())
    }

    fn process(&mut self, stream: TcpStream) -> Result<()> {
        info!("accept connection: {}", stream.peer_addr()?);
        let mut reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);

        let mut buffer = [0; 1024];
        loop {
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
                    info!("req: {}", req);
                    match req {
                        Request::Get { key } => match self.engine.get(key) {
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

                        Request::Set { key, value } => match self.engine.set(key, value) {
                            Ok(_) => {
                                writer
                                    .write(Reply::SingleLine("".to_string()).to_resp().as_ref())?;
                                writer.flush()?;
                            }
                            Err(e) => {
                                writer.write(Reply::Err(e.to_string()).to_resp().as_ref())?;
                                writer.flush()?;
                            }
                        },
                        Request::Remove { key } => match self.engine.remove(key) {
                            Ok(_) => {
                                writer
                                    .write(Reply::SingleLine("".to_string()).to_resp().as_ref())?;
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
}

fn handle<T: KvsEngine>(engine: T, stream: TcpStream) -> Result<()> {
    info!("accept connection: {}", stream.peer_addr()?);
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let mut buffer = [0; 1024];
    loop {
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
                info!("req: {}", req);
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

                    Request::Set { key, value } => match engine.set(key, value) {
                        Ok(_) => {
                            writer.write(Reply::SingleLine("".to_string()).to_resp().as_ref())?;
                            writer.flush()?;
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
