use crate::{
    Message,
    state::{Db, DbEvent, DbEventListener},
};
use futures::SinkExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info};

///The taxicab server state.
#[derive(Debug)]
pub struct Taxicab {
    listener: TcpListener,
}

enum ServerCommand {
    NewClient {
        addr: String,
        sender: UnboundedSender<Message>,
    },
    MessageReceived {
        message: Vec<u8>,
        from_addr: String,
    },
    SendMessage {
        message: Message,
        to_addr: String,
    },
}

impl Taxicab {
    ///Create a new taxicab server instance by the given TcpListener
    pub fn new(listener: TcpListener) -> Self {
        Self { listener }
    }

    ///Run the taxicab server instance
    ///
    ///This method is instrumented by the `tracing` crate.
    ///
    #[tracing::instrument]
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel::<ServerCommand>();

        let (db, db_event_listener) = Db::new();

        let mut dispatcher = Dispatcher::new(db_event_listener, tx.clone());

        let dispatcher_db = db.instance();
        tokio::spawn(async move {
            dispatcher.run(dispatcher_db).await;
        });

        let mut db = db.instance();
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    ServerCommand::NewClient { addr, sender } => {
                        debug!(Address = addr, "A new client is connected to the server");
                        db.keep_endpoint_in_loop(addr.to_string(), sender);
                    }

                    ServerCommand::SendMessage { message, to_addr } => {
                        debug!(Address = to_addr, "Dispatching a message to a client");
                        if let Err(_e) = db.forward_message_to(message, &to_addr) {
                            //error log;
                        }
                    }

                    ServerCommand::MessageReceived { message, from_addr } => {
                        Self::on_message_received(message, from_addr, &mut db);
                    }
                }
            }
        });

        loop {
            info!("roaming around maybe a passenger hailing ...");

            if let Ok((socket, addr)) = self.listener.accept().await {
                let message_writer = tx.clone();
                tokio::spawn(async move {
                    Self::handle_socket(socket, addr.to_string(), message_writer).await;
                });
            }
        }
    }

    fn on_message_received(message: Vec<u8>, from_addr: String, db: &mut Db) {
        debug!(Address = from_addr, "A new message received from a client");

        let bytes = message.clone();
        match Message::from_bytes(message.as_slice()) {
            Ok(message) => match message {
                Message::Act(_) => {}
                Message::Binding(exchange) => {
                    db.bind(&exchange, from_addr);
                }
                Message::Request(message) => {
                    db.enqueue(message.exchange(), bytes);
                }
            },
            Err(_e) => {
                error!("Failed to create a `Message` from the given bytes");
            }
        }
    }

    async fn handle_socket(
        socket: TcpStream,
        addr: String,
        command_sender: UnboundedSender<ServerCommand>,
    ) {
        let mut framed_socket = Framed::new(socket, LengthDelimitedCodec::new());

        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        if let Ok(_) = command_sender.send(ServerCommand::NewClient {
            addr: addr.to_string(),
            sender: tx,
        }) {
            loop {
                tokio::select! {
                   Some(Ok(bytes))= framed_socket.next() => {

                        if let Ok(_) = command_sender.send(ServerCommand::MessageReceived {
                            message: bytes.to_vec(),
                            from_addr: addr.to_string(),
                        }) {}
                    }

                    Some(message) = rx.recv() => {
                        let _ = framed_socket.send(message.to_bytes().into()).await;
                    }
                };
            }
        }
    }
}

struct Dispatcher {
    listener: UnboundedReceiver<(String, DbEventListener)>,
    command_sender: UnboundedSender<ServerCommand>,
}

impl Dispatcher {
    pub(crate) fn new(
        listener: UnboundedReceiver<(String, UnboundedReceiver<DbEvent>)>,
        command_sender: UnboundedSender<ServerCommand>,
    ) -> Self {
        Self {
            listener,
            command_sender,
        }
    }

    pub async fn run(&mut self, db: Db) {
        while let Some((exchange, listener)) = self.listener.recv().await {
            debug!(exchange = exchange, "Message dispatcher started for");

            self.spawn_exchange_dispatcher(db.instance(), listener);
        }
    }

    fn spawn_exchange_dispatcher(&self, db: Db, mut listener: UnboundedReceiver<DbEvent>) {
        let command_sender = self.command_sender.clone();
        let mut db = db.instance();
        tokio::spawn(async move {
            while let Some(event) = listener.recv().await {
                match event {
                    DbEvent::NewMessage(exchange) => {
                        let client = db.who_handles(&exchange).next();
                        if let Some(client) = client {
                            if let Some(message) = db.dequeue(&exchange) {
                                let _ = command_sender.send(ServerCommand::SendMessage {
                                    message: Message::from_bytes(&message).unwrap(),
                                    to_addr: client.to_string(),
                                });
                            }
                            debug!(
                                exchange = exchange,
                                clients = client,
                                "A new messsage added event"
                            );
                        }
                    }
                }
            }
        });
    }
}

///run a taxicab server insatnce by the given TcpListener.
///
///```rust
///use std::error::Error;
///use taxicab::run;
///use tokio::net::TcpListener;
///use tracing::Level;
///use tracing_subscriber::FmtSubscriber;
///
///#[tokio::main]
///async fn main() -> Result<(), Box<dyn Error>> {
///    // a builder for `FmtSubscriber`.
///    let subscriber = FmtSubscriber::builder()
///        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
///        // will be written to stdout.
///        .with_max_level(Level::TRACE)
///        // completes the builder.
///        .finish();
///
///    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
///
///    let listener = TcpListener::bind("127.0.0.1:1729").await?;
///    run(listener).await;
///
///    Ok(())
///}
///```
///
pub async fn run(listener: TcpListener) {
    let _ = Taxicab::new(listener).run().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn server_basic_work() {
        let addr = format!("127.0.0.1:{}", 1729);

        if let Ok(listener) = TcpListener::bind(addr).await {
            run(listener).await;
        }
    }
}
