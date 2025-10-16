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

    let ack_sender = message_sender.clone();


    tokio::spawn(async move {
        while let Some(message) = message_receiver.recv().await {
            match message {
                taxicab::Message::Request(message) => {
                    info!(Message = message.content(), "Message received");
                    let _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
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
                    info!(
                        message_id = message_id.to_string(),
                        "the message processing should be canceled for"
                    );
                }
                _ => {}
            }
        }
    });

    let exchange = "some-exchange";

    //do not bind , allow consume.rs to do this as a consumer of the message send by this client
    message_sender.bind(exchange).await?;

    let mut counter = 1;
    loop {
        let message = format!("taxi, timestamp: {}, producer", counter);
        message_sender
            .send(message.as_str(), exchange)
            .await?;

        info!(message = message, "sent");

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        counter += 1;
    }

    info!("going to shutdown");

    Ok(())
}
