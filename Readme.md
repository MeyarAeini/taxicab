# Taxicab

Taxicab is a message broker. This project is a work-inprogress.

## Why taxicab

This name was more about the number first but I used the name as the application name and the number as the default port.
I remember when I was child I found a biography book about Gofrey Harold Hardy (Mathematician) in our small library. I started reading this book and I found something which amazed me and I would never forget: `A number can be so important`, Numbers have their own personality. There was a conversation in the hospital between two mathematicians about a taxicab number : **1729**.

`I remember once going to see him [Ramanujan] when he was lying ill at Putney. I had ridden in taxi-cab No. 1729, and remarked that the number seemed to be rather a dull one, and that I hoped it was not an unfavourable omen. "No," he replied, "it is a very interesting number; it is the smallest number expressible as the sum of two cubes in two different ways."`

[taxicab number](https://en.wikipedia.org/wiki/Taxicab_number)

I always wanted to use this name for something and finally I thought this is a greate name for a message broker service. I started this project for learning purpose and exploring distibuted service developement using Rust. That does not mean this project wont be used in production one day, I'll continue improve the code quality and add different features to it. 

In this project I have used [Tokio](https://tokio.rs).

## Server

To run the server you can run the following command. This runs the taxicab server with default port 1729. 

```bash
cargo r 
```

You can also write your own server with the port that you want by creating a new binary Rust application :
```rust
use std::error::Error;

use taxicab::run;
use tokio::net::TcpListener;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let listener = TcpListener::bind("127.0.0.1:1729").await?;
    run(listener).await;

    Ok(())
}
```

## Clients

Clients of the taxicab server can do different tasks. They can be a simple message producer, or a simple message consumer or both. At the moment `taxicab` does not have publish-subscribe functionalities.

### A message producer

```bash
cargo r --example producer
```

You can also write your own message producer like the following example.

```rust
use std::{
    error::Error,
    net::SocketAddr,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use taxicab::{
    MessageHandler, MessageHandlerRegistry, MessagePath, TaxicabClient,
};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Serialize, Deserialize)]
struct MyMessage {
    content: String,
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
    let registry = MessageHandlerRegistry::new();

    let message_path = MessagePath::new(format!("some-exchange"), format!("my-message"));

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
```

### A message consumer

```bash
cargo r --example consumer
```

You can also write your own message consumer like the following example.

```rust
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
```


## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `taxicab` by you, shall be licensed as MIT, without any
additional terms or conditions.
