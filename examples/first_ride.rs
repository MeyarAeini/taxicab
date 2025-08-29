use std::error::Error;

use taxicab::PassengerHandler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut handler = PassengerHandler::hail("127.0.0.1:1729").await?;

    handler.send("take me home, please!").await?;

    Ok(())
}
