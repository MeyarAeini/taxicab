use std::error::Error;

use taxicab::connect;
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

    let (message_sender, mut message_receiver) = connect("127.0.0.1:1729").await?;

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

    //do not bind , allow consume.rs to do this as a consumer of the message send by this client
    //client.bind(exchange).await?;

    for _i in 0..5 {
        message_sender
            .send("take me home, please!", exchange)
            .await?;

        info!("A message sent to the server");

        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }

    info!("going to shutdown");

    Ok(())
}
