use std::error::Error;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub struct Trip {
    stream: TcpStream,
}

impl Trip {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn write(&mut self, message: &str) -> Result<(), Box<dyn Error>> {
        println!("this trip wants to do:   {} ", message);

        let bytes = message.as_bytes();

        println!("writing {:#?}", bytes);
        self.stream.write(bytes).await?;
        Ok(())
    }

    pub async fn read(&mut self) -> Result<String, Box<dyn Error>> {
        let mut buffer = [0; 1024];

        let n = self.stream.read(&mut buffer).await?;

        if n == 0 {
            println!("connection closed by client");
        } else {
            println!("read {} bytes : {:#?}", n, &buffer[0..n]);
            return Ok(str::from_utf8(&buffer[0..n])?.to_string());
        }

        Ok(String::new())
    }
}
