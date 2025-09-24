use std::collections::HashMap;

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, ReadHalf},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedSender},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::SinkExt;
use tokio_stream::StreamExt;

use crate::{Message, message};

//use crate::taxicab_connection::TaxicabConnection;

pub struct Taxicab {
    listener: TcpListener,
}

struct TaxicabHandler {
    reader: ReadHalf<TcpStream>,
    addr: String,
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
    pub fn new(listener: TcpListener) -> Self {
        Self { listener }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel::<ServerCommand>();

        let tx_handler = tx.clone();
        tokio::spawn(async move {
            let mut endpoints = HashMap::new();
            while let Some(command) = rx.recv().await {
                match command {
                    ServerCommand::NewClient { addr, sender } => {
                        println!("running command , NewClient({})", addr);
                        endpoints.insert(addr.to_string(), sender);
                    }

                    ServerCommand::SendMessage { message, to_addr } => {
                        println!("running command , SendMessage({:#?},{})", message, to_addr);

                        if let Some(sender) = endpoints.get(&to_addr.to_string()) {
                            let _ = sender.send(message);
                        }
                    }

                    ServerCommand::MessageReceived { message, from_addr } => {
                        println!(
                            "running command, MessageReceived({:#?},{})",
                            message, from_addr
                        );

                        match Message::from_bytes(message.as_slice()) {
                            Ok(message) => {
                                let message = Message::new(message.header.exchange.clone(), format!(
                                        "I received your message. Message : {:#?}. Here is your address if you did not know: {}",
                                        message, from_addr
                                    ));

                                let message = ServerCommand::SendMessage {
                                    message,                                     to_addr: from_addr,
                                };
                                let _ = tx_handler.clone().send(message);
                            }
                            Err(e) => {
                                println!("{:#?}", e);
                            }
                        }
                    }
                }
            }
        });

        loop {
            println!("roaming around maybe a passenger hailing ...");

            if let Ok((socket, addr)) = self.listener.accept().await {
                let message_writer = tx.clone();
                tokio::spawn(async move {
                    Self::handle_socket(socket, addr.to_string(), message_writer).await;
                });
            }
        }
    }
    async fn handle_socket(
        socket: TcpStream,
        addr: String,
        command_sender: UnboundedSender<ServerCommand>,
    ) {
        println!("a new passenger hailed, this is the address : {}", addr);

        //let (mut reader, mut writer) = tokio::io::split(socket);
        let mut framed_socket = Framed::new(socket, LengthDelimitedCodec::new());

        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        if let Ok(_) = command_sender.send(ServerCommand::NewClient {
            addr: addr.to_string(),
            sender: tx,
        }) {
            //let mut reader = BufReader::new(reader).lines();
            loop {
                //let mut buffer = [0u8; 2048];
                tokio::select! {
                    //println!("waiting for message from {}", addr.to_string());
                   Some(Ok(bytes))= framed_socket.next() => {

                       //let bytes : Vec<u8>= buffer[..n].to_owned();
                        if let Ok(_) = command_sender.send(ServerCommand::MessageReceived {
                            message: bytes.to_vec(),
                            from_addr: addr.to_string(),
                        }) {}

                        //the queue engine here receives a new message , it also should be conscious
                        //about dispatchining the message to the destination address using self.senders
                        //A message should have a property which define the queue that this message
                        //should be put it. message from different type still can go the same queue, no
                        //limitation on this
                    }

                    Some(message) = rx.recv() => {
                        let _ = framed_socket.send(message.to_bytes().into()).await;
                        //if !writer.write_all(message.to_bytes().as_slice()).await.is_err() {
                        //    let _ = writer.write_all(b"\n").await;
                        //}
                    }
                };
            }
        }
    }
}

impl TaxicabHandler {
    fn new(addr: String, reader: ReadHalf<TcpStream>) -> Self {
        Self { reader, addr }
    }
    async fn run(&mut self) {
        //let message = self.connection.read().await?;

        //println!("A new passenger has received a service, here is their request detail:");
        //println!("{}", message);
    }
}

pub async fn run(listener: TcpListener) {
    Taxicab::new(listener).run().await;
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
