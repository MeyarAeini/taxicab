//!
//!`taxicab` is a crate to eventually do a message bus funtionalities. I am discovering `rust`
//!during creating this crate so the code might evolve and change deeply after each iteration.
//!

#![deny(missing_docs)]

mod client;
mod controller;
mod dispatcher;
mod message;
mod server;
mod state;

///
///The client struct.
///
///By using this struct, a new client to connect to the `taxicab` server will be configured and
///initiated.
///
pub use client::{MessageHandler, MessageHandlerAdapter, TaxicabBuilder, TaxicabClient};

///
///This struct is the frame of each message transported on each server/client intraction for
///`taxicab`
///
pub use message::{Message, MessagePath};

///
///To initiate a `taxicab` server instance this method can be used
///```rust
///use taxicab::run;
///use tokio::net::TcpListener;
///
///#[tokio::main]
///async fn main() -> Result<(), Box<dyn Error>> {
///    let listener = TcpListener::bind("127.0.0.1:1729").await?;
///    run(listener).await;
///
///    Ok(())
///}
///```
///
pub use server::run;
