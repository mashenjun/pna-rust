use crate::{Command, KvsError, Result};
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

pub struct KvsClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn new(addr: SocketAddr) -> Result<Self> {
        let tcp_reader = TcpStream::connect_timeout(&addr, Duration::from_millis(1000))?;

        let tcp_writer = tcp_reader.try_clone()?;
        Ok(KvsClient {
            reader: BufReader::new(tcp_reader),
            writer: BufWriter::new(tcp_writer),
        })
    }

    pub fn process(&mut self, cmd: &Command) -> Result<String> {
        serde_json::to_writer(&mut self.writer, cmd)?;
        self.writer.flush()?;
        info!("flush cmd {}", cmd);
        let mut buffer = String::new();
        match self.reader.read_line(&mut buffer) {
            Err(e) => {
                error!("{}", e);
                return Err(KvsError::from(e));
            }
            Ok(cnt) => {
                info!("{}", cnt);
                info!("{}", buffer);
            }
        }
        info!("cmd {} done", cmd);
        Ok(buffer)
    }
}
