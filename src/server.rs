use std::error::Error;

use tokio::net::TcpListener;

use crate::trip::Trip;

pub struct Taxicab {
    listener: TcpListener,
}

struct Chauffeur {
    trip: Trip,
}

impl Taxicab {
    pub fn new(listener: TcpListener) -> Self {
        Self { listener }
    }

    pub async fn run(&self) {
        loop {
            println!("roaming around maybe a passenger hailing ...");
            match self.listener.accept().await {
                Ok((socket, _)) => {
                    let mut handler = Chauffeur {
                        trip: Trip::new(socket),
                    };

                    tokio::spawn(async move {
                        match handler.run().await {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Error occurred on handling the message. Detail:{:#?}", e);
                            }
                        }
                    });
                }
                Err(e) => {
                    println!("error happend: {:#?}", e);
                }
            }
        }
    }
}

impl Chauffeur {
    async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        let message = self.trip.read().await?;

        println!("A new passenger has received a service, here is their request detail:");
        println!("{}", message);

        Ok(())
    }
}

pub async fn run(listener: TcpListener) {
    Taxicab::new(listener).run().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn server_basic_work() {
        let addr = format!("127.0.0.1:{}", 1729);

        if let Ok(listener) = TcpListener::bind(addr).await {
            run(listener).await;
        }
    }
}
