use std::{error::Error, net::SocketAddr, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use taxicab::{
    Driving, MessageHandler, MessageHandlerAdapter, MessageHandlerRegistry, MessagePath,
    TaxicabClient,
};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Serialize, Deserialize)]
struct SalesMessage {
    no: i32,
    name: String,
    price: f64,
}

#[derive(Serialize, Deserialize)]
struct TestMessage {
    content: String,
}

struct TestMessageHandler;

fn get_randoms() -> (i32, f64) {
    use rand::prelude::*;

    let mut rng = rand::rng();

    (rng.random::<i32>(), rng.random::<f64>())
}

#[async_trait]
impl<'de> MessageHandler<'de> for TestMessageHandler {
    type Error = anyhow::Error;
    type Message = TestMessage;

    async fn handle(
        &self,
        taxicab: &TaxicabClient<Driving>,
        message: Self::Message,
    ) -> Result<(), Self::Error> {
        info!(
            message = message.content,
            "A message received on the client side"
        );

        let (no, price) = get_randoms();

        let _ = taxicab
            .send(
                &SalesMessage {
                    no,
                    name: message.content,
                    price,
                },
                MessagePath::new(format!("Sales"), format!("ProductSoldCommand")),
            )
            .await;

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
        MessagePath::new(format!("producer"), format!("test-message")),
        MessageHandlerAdapter::new(TestMessageHandler),
    );

    let client = TaxicabClient::new(addr, registry);
    let _ = client.connect().await;

    info!("connected to the server");

    let _ = tokio::time::sleep(Duration::from_secs(200)).await;

    info!("going to shutdown");

    Ok(())
}
