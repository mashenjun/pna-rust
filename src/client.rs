use crate::{parse_reply, KvsError, Reply, Request, Result};
use std::io::{BufRead, BufReader, BufWriter, Write};
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

    pub fn process(&mut self, req: &Request) -> Result<Reply> {
        self.writer.write(req.to_resp().as_ref())?;
        self.writer.flush()?;
        let mut buffer = String::new();
        let cnt = self.reader.read_line(&mut buffer)?;
        info!("cnt {}", cnt);
        let reply = parse_reply(buffer.as_str());
        return match reply {
            Err(_) => Err(KvsError::InvalidCommandError),
            Ok((_, reply)) => Ok(reply),
        };
    }
}
