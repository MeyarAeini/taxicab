use std::error::Error;

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
            info!(Message = format!("{:#?}", message.body), "Message received");
        }
    });
    for _i in 0..5 {
        client.send("take me home, please!").await?;

        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }

    info!("going to shutdown");

    Ok(())
}
