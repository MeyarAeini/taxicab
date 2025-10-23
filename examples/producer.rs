use std::{error::Error, fmt::format, net::SocketAddr};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use taxicab::{
    Driving, MessageHandler, MessageHandlerAdapter, MessageHandlerRegistry, MessagePath,
    TaxicabClient,
};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Serialize, Deserialize)]
struct ProducerMessage {
    content: String,
}

#[derive(Serialize, Deserialize)]
struct SalesMessage {
    no: i32,
    name: String,
    price: f64,
}

struct SalesMessageHandler;

#[async_trait]
impl<'de> MessageHandler<'de> for SalesMessageHandler {
    type Error = anyhow::Error;
    type Message = SalesMessage;

    async fn handle(
        &self,
        _: &TaxicabClient<Driving>,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        info!(
            no = message.no,
            name = message.name,
            price = message.price,
            "Received a sales message"
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

    let address = SocketAddr::from(([127, 0, 0, 1], 1729));
    let mut registry = MessageHandlerRegistry::new();

    registry.insert(
        MessagePath::new(format!("Sales"), format!("ProductSoldCommand")),
        MessageHandlerAdapter::new(SalesMessageHandler),
    );

    let client = TaxicabClient::new(address, registry);

    if let Ok(client) = client.connect().await {
        loop {
            let message = ProducerMessage {
                content: format!("hi there!"),
            };
            let _ = client
                .send(
                    &message,
                    MessagePath::new(format!("producer"), format!("test-message")),
                )
                .await;

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    info!("going to shutdown");

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    Ok(())
}
