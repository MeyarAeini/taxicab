use std::error::Error;

use taxicab::TaxicabClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    //let client = TaxicabClient::connect("127.0.0.1:1729", handle).await?;
    //
    let client = taxicab::XiClient::connect("127.0.0.1:1729").await?;

    println!("connected to the server");

    for i in 0..5 {
        println!("sending {}", i);
        client.send("take me home, please!").await?;

        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }

    println!("going to shutdown");

    //client.shutdown().await;

    Ok(())
}

fn handle(message: String) {
    println!("{}", message);
}
