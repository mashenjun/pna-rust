use crate::{Command, KvsEngine, Result};
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// The server of a key value store.
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    pub fn run(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => self.process(stream)?,
                Err(e) => error!("connection failed: {}", e),
            }
        }
        Ok(())
    }

    fn process(&self, stream: TcpStream) -> Result<()> {
        info!("accept connection: {}", stream.peer_addr()?);
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let payload_reader = Deserializer::from_reader(reader).into_iter::<Command>();

        for cmd in payload_reader {
            // echo
            let cmd = cmd?;
            info!("cmd: {}", cmd);
            writer.write(cmd.to_string().as_ref())?;
            writer.write(b"\r\n");
            writer.flush()?;
            info!("echo flush: {}", cmd);
            // match cmd {
            //     Command::Get { key } => match self.engine.get(key) {
            //         Ok(res) => {
            //             serde_json::to_writer(&mut writer, &res)?;
            //             writer.flush()?;
            //         }
            //         Err(e) => {
            //             writer.write(format!("{}", e).as_ref())?;
            //             writer.flush()?;
            //         }
            //     },
            //     Command::Set { key, value } => match self.engine.set(key, value) {
            //         Ok(_) => {
            //             serde_json::to_writer(&mut writer, "Ok")?;
            //             writer.flush()?;
            //         }
            //         Err(e) => {
            //             writer.write(format!("{}", e).as_ref())?;
            //             writer.flush()?;
            //         }
            //     },
            //     Command::Remove { key } => match self.engine.remove(key) {
            //         Ok(_) => {
            //             serde_json::to_writer(&mut writer, "Ok")?;
            //             writer.flush()?;
            //         }
            //         Err(e) => {
            //             writer.write(format!("{}", e).as_ref())?;
            //             writer.flush()?;
            //         }
            //     },
            // }
        }
        Ok(())
    }
}
