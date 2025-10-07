use crate::{
    Message,
    controller::{ControllerListener, run_controller},
};
use futures::SinkExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::info;

///The taxicab server state.
#[derive(Debug)]
pub struct Taxicab {
    listener: TcpListener,
}

impl Taxicab {
    ///Create a new taxicab server instance by the given TcpListener
    pub fn new(listener: TcpListener) -> Self {
        Self { listener }
    }

    ///Run the taxicab server instance
    ///
    ///This method is instrumented by the `tracing` crate.
    ///
    #[tracing::instrument]
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let controller = run_controller().await?;

        loop {
            info!("roaming around maybe a passenger hailing ...");

            if let Ok((socket, addr)) = self.listener.accept().await {
                let controller = controller.clone();
                tokio::spawn(async move {
                    Self::handle_socket(socket, addr.to_string(), controller).await;
                });
            }
        }
    }

    async fn handle_socket(socket: TcpStream, addr: String, controller: ControllerListener) {
        let mut framed_socket = Framed::new(socket, LengthDelimitedCodec::new());

        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        if let Ok(_) = controller.new_client(addr.to_string(), tx) {
            loop {
                tokio::select! {
                   Some(Ok(bytes))= framed_socket.next() => {

                        if let Ok(_) = controller.message_received(
                            bytes.to_vec(),
                            addr.to_string(),
                        ) {}
                    }

                    Some(message) = rx.recv() => {
                        let _ = framed_socket.send(message.to_bytes().into()).await;
                    }
                };
            }
        }
    }
}

///run a taxicab server insatnce by the given TcpListener.
///
///```rust
///use std::error::Error;
///use taxicab::run;
///use tokio::net::TcpListener;
///use tracing::Level;
///use tracing_subscriber::FmtSubscriber;
///
///#[tokio::main]
///async fn main() -> Result<(), Box<dyn Error>> {
///    // a builder for `FmtSubscriber`.
///    let subscriber = FmtSubscriber::builder()
///        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
///        // will be written to stdout.
///        .with_max_level(Level::TRACE)
///        // completes the builder.
///        .finish();
///
///    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
///
///    let listener = TcpListener::bind("127.0.0.1:1729").await?;
///    run(listener).await;
///
///    Ok(())
///}
///```
///
pub async fn run(listener: TcpListener) {
    let _ = Taxicab::new(listener).run().await;
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
