use std::error::Error;

use tokio::net::TcpStream;

use crate::trip::Trip;

pub struct PassengerHandler {
    trip: Trip,
}

impl PassengerHandler {
    pub async fn hail(addr: &str) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            trip: Trip::new(stream),
        })
    }

    pub async fn send(&mut self, message: &str) -> Result<(), Box<dyn Error>> {
        self.trip.write(message).await?;

        Ok(())
    }
}
