use std::{error::Error, net::SocketAddr, time::Duration};

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
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
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

    let addr = SocketAddr::from(([127, 0, 0, 1], 1729));

    let mut registry = MessageHandlerRegistry::new();
    registry.insert(
        MessagePath::new(format!("some-exchange"), format!("my-message")),
        MessageHandlerAdapter::new(MyMessageHanler),
    );

    let client = TaxicabClient::new(addr, registry);
    let _ = client.connect().await;

    info!("connected to the server");

    let _ = tokio::time::sleep(Duration::from_secs(200)).await;

    info!("going to shutdown");

    Ok(())
}
