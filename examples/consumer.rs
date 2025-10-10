use std::{error::Error, time::Duration};

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

    let ack_sender = message_sender.clone();

    tokio::spawn(async move {
        while let Some(message) = message_receiver.recv().await {
            match message {
                taxicab::Message::Request(message) => {
                    info!(Message = message.content(), "Message received");
                    let _ = tokio::time::sleep(Duration::from_secs(5)).await;
                    let _ = ack_sender
                        .ack(
                            message
                                .message_id()
                                .parse()
                                .expect("failed parsing &str to MessageId"),
                        )
                        .await;
                }
                taxicab::Message::Cancellation(message_id) => {
                    info!(message_id=message_id.to_string(), "the message processing should be canceled for");
                }
                _ => {}
            }
        }
    });

    let exchange = "some-exchange";

    message_sender.bind(exchange).await?;

    let _ = tokio::time::sleep(Duration::from_secs(200)).await;

    info!("going to shutdown");

    Ok(())
}
