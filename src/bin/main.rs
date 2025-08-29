use std::error::Error;

use taxicab::run;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:1729").await?;
    run(listener).await;

    Ok(())
}
