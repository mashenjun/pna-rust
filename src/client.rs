use crate::Result;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn new(addr: SocketAddr) -> Result<Self> {
        let tcp_reader = TcpStream::connect_timeout(&addr, Duration::from_millis(10))?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(KvsClient {
            reader: Deserializer::from_reader(BufReader::new(tcp_reader)),
            writer: BufWriter::new(tcp_writer),
        })
    }
}
