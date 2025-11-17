use std::{error::Error, net::SocketAddr};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use taxicab::{
    MessageHandler, MessageHandlerAdapter, MessagePath, TaxicabBuilder, TaxicabClient,
    TaxicabHandlerProfile,
};
use tokio::signal::ctrl_c;
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

    async fn handle(&self, _: &TaxicabClient, message: Self::Message) -> Result<(), Self::Error> {
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

    let _ = TaxicabBuilder::new(address)
        .with_task(move |taxicab| {
            Box::pin(async move {
                loop {
                    let message = ProducerMessage {
                        content: format!("hi there!"),
                    };
                    let _ = taxicab
                        .send(
                            &message,
                            MessagePath::new(format!("producer"), format!("test-message")),
                        )
                        .await;

                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            })
        })
        .with_handler(
            MessagePath {
                exchange: format!("Sales"),
                local_path: format!("ProductSoldCommand"),
            },
            TaxicabHandlerProfile::new(MessageHandlerAdapter::new(SalesMessageHandler)),
        )
        .connect(ctrl_c())
        .await;
    info!("going to shutdown");

    Ok(())
}
