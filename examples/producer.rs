use std::{
    error::Error,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use taxicab::{
    MessageHandler, MessageHandlerAdapter, MessageHandlerRegistry, MessagePath, TaxicabClient,
};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Serialize, Deserialize)]
struct MyMessage {
    content: String,
}

struct MyMessageHanler;

#[async_trait]
impl<'de> MessageHandler<'de> for MyMessageHanler {
    type Error = anyhow::Error;
    type Message = MyMessage;

    async fn handle(&self, message: Self::Message) -> Result<(), Self::Error> {
        info!(
            message = message.content,
            "A message received on the client side"
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        Ok(())
    }
}
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

    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1729));
    let mut registry = MessageHandlerRegistry::new();

    let message_path = MessagePath::new(format!("some-exchange"), format!("my-message"));

    registry.insert(
        message_path.clone(),
        MessageHandlerAdapter::new(MyMessageHanler),
    );

    let client = TaxicabClient::new(address, registry);

    if let Ok(client) = client.connect().await {
        loop {
            let message = MyMessage {
                content: format!("hi there!"),
            };
            let _ = client.send(&message, message_path.clone()).await;

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    info!("going to shutdown");

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    Ok(())
}
