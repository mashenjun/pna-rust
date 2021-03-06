use crate::{parse_reply, KvsError, Reply, Request, Result};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

pub struct KvsClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn new(addr: SocketAddr) -> Result<Self> {
        let socket = Socket::new(Domain::ipv4(), Type::stream(), Some(Protocol::tcp()))?;
        if let Err(e) = socket.connect_timeout(&SockAddr::from(addr), Duration::from_millis(3000)) {
            error!("connect fail {}", e);
            return Err(KvsError::from(e));
        }
        socket.set_linger(Some(Duration::new(0, 0)))?;
        let connection = socket.into_tcp_stream();
        Ok(KvsClient {
            reader: BufReader::new(connection.try_clone()?),
            writer: BufWriter::new(connection.try_clone()?),
        })
    }

    pub fn process(&mut self, req: &Request) -> Result<Reply> {
        self.writer.write_all(req.to_resp().as_ref())?;
        self.writer.flush()?;
        let mut buffer = String::new();
        let cnt = self.reader.read_line(&mut buffer)?;
        debug!("cnt {}", cnt);
        let reply = parse_reply(buffer.as_str());
        match reply {
            Err(_) => Err(KvsError::InvalidCommandError),
            Ok((_, reply)) => Ok(reply),
        }
    }
}
