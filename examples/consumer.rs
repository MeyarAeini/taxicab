use std::{error::Error, time::Duration};

use taxicab::TaxicabClient;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let (client, mut message_receiver) = TaxicabClient::connect("127.0.0.1:1729").await?;

    info!("connected to the server");

    tokio::spawn(async move {
        while let Some(message) = message_receiver.recv().await {
            match message {
                taxicab::Message::Request(message) => {
                    info!(Message = message.content(), "Message received");
                }
                _ => {}
            }
        }
    });

    let exchange = "some-exchange";

    client.bind(exchange).await?;

    let _ = tokio::time::sleep(Duration::from_secs(200)).await;

    info!("going to shutdown");

    Ok(())
}
