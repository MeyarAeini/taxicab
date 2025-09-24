use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, WriteHalf},
    join,
    net::TcpStream,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::Message;

pub struct TaxicabAddr {
    addr: String,
}

pub struct TaxicabClient {
    writer: UnboundedSender<String>,
    read_handle: JoinHandle<()>,
    write_hanlde: JoinHandle<()>,
}

enum ClientCommand {
    MessageReceived(Message),
    SendMessage(Message),
}

pub struct XiClient {
    sender: UnboundedSender<ClientCommand>,
}

impl XiClient {
    fn command_processor(
        message_sender: UnboundedSender<Message>,
        message_receiver: UnboundedSender<Message>,
    ) -> UnboundedSender<ClientCommand> {
        let (tx, mut rx) = mpsc::unbounded_channel::<ClientCommand>();

        let sender = tx.clone();

        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    ClientCommand::SendMessage(message) => {
                        println!("sending ... {:#?}", message);
                        let _ = message_sender.send(message);
                    }
                    ClientCommand::MessageReceived(message) => {
                        println!("new message received: {:#?}", message);
                        let _ = message_receiver.send(message);
                    }
                }
            }
        });

        sender
    }

    pub async fn connect(addr: &str) -> anyhow::Result<(Self, UnboundedReceiver<Message>)> {
        let stream = TcpStream::connect(addr).await?;

        let mut framed_socket = Framed::new(stream, LengthDelimitedCodec::new());
        //let (mut reader, mut writer) = tokio::io::split(stream);

        //let mut reader = BufReader::new(reader).lines();

        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        let (tx_receiver, mut rx_receiver) = mpsc::unbounded_channel::<Message>();

        let command_sender = Self::command_processor(tx.clone(), tx_receiver);

        let command_sender_copy = command_sender.clone();
        tokio::spawn(async move {
            loop {
               // let mut buffer = [0u8; 2048];
                tokio::select! {

                    Some(Ok(bytes)) = framed_socket.next() => {
                        match Message::from_bytes(&bytes[..]) {
                            Ok(message) => {
                        let _ = command_sender.send(ClientCommand::MessageReceived(message));
                            },
                            Err(e) => {
                                println!("{:#?}", e);
                            }
                        }
                    }

                    //Ok(Some(line)) = reader.next_line() => {
                    //    let _ = command_sender.send(ClientCommand::MessageReceived(line));
                    //}

                    Some(message) = rx.recv() => {
                        if let Err(e) =framed_socket.send(message.to_bytes().into()).await {
                            println!("an error happend on sending a message. {:#?}", e);
                        }
                    }
                }
            }
        });

        Ok((
            Self {
                sender: command_sender_copy,
            },
            rx_receiver,
        ))
    }
    pub async fn send(&self, message: &str) -> anyhow::Result<()> {
        let message = Message::new("some-exchange".to_string(), message.to_string());
        self.sender.send(ClientCommand::SendMessage(message))?;

        Ok(())
    }
}

impl TaxicabClient {
    pub async fn connect<F>(addr: &str, func: F) -> anyhow::Result<Self>
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let stream = TcpStream::connect(addr).await?;

        let (reader, mut writer) = tokio::io::split(stream);

        let mut reader = BufReader::new(reader).lines();

        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        let read_task = tokio::spawn(async move {
            while let Ok(Some(line)) = reader.next_line().await {
                //TODO: deserialize the new message and pass it to handlers
                println!("{}", line);
                (func)(line);
            }
        });

        let write_task = tokio::spawn(async move {
            while let Some(new_message) = rx.recv().await {
                if writer.write_all(new_message.as_bytes()).await.is_err() {
                    break;
                }
                let _ = writer.write_all(b"\n").await;
            }
        });

        Ok(Self {
            writer: tx,
            read_handle: read_task,
            write_hanlde: write_task,
        })
    }

    pub async fn shutdown(self) {
        drop(self.writer);

        let _ = join!(self.read_handle, self.write_hanlde);
    }

    pub async fn send(&self, message: String) -> anyhow::Result<()> {
        println!("sending a new message {}", message.to_string());
        let _ = self.writer.send(message)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_test() {
        let client = TaxicabClient::connect("127.0.0.1::1729", |message| {
            println!("{}", message);
        })
        .await;

        if let Ok(client) = client {
            client.send(String::new()).await;
        }
    }
}

//pub struct Passenger {
//    connection: TaxicabConnection,
//}
//
//trait TaxicabClient {
//}
//
//impl Passenger {
//    pub async fn connect(addr: &str) -> Result<Self, Box<dyn Error>> {
//        let stream = TcpStream::connect(addr).await?;
//        Ok(Self {
//            connection: TaxicabConnection::new(stream),
//        })
//    }
//
//    pub async fn send(&mut self, message: &str) -> Result<(), Box<dyn Error>> {
//        self.connection.write(message).await?;
//
//        Ok(())
//    }
//}
//
//pub struct Chauffeur {
//    connection: TaxicabConnection,
//}
//
//impl Chauffeur {
//    pub async fn connect(addr: &str) -> Result<Self, Box<dyn Error>> {
//        let stream = TcpStream::connect(addr).await?;
//        Ok(Self {
//            connection: TaxicabConnection::new(stream),
//        })
//    }
//
//    pub async fn send(&mut self, message: &str) -> Result<(), Box<dyn Error>> {
//        self.connection.write(message).await?;
//
//        Ok(())
//    }
//
//}
