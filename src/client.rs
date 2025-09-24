use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::Message;
use tracing::{debug, error};

enum ClientCommand {
    MessageReceived(Message),
    SendMessage(Message),
}

pub struct TaxicabClient {
    sender: UnboundedSender<ClientCommand>,
}

impl TaxicabClient {
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
                        debug!(Message = message.body, " Sending a message to the server");
                        let _ = message_sender.send(message);
                    }
                    ClientCommand::MessageReceived(message) => {
                        debug!(
                            Message = message.body,
                            "A new message received from the server"
                        );
                        let _ = message_receiver.send(message);
                    }
                }
            }
        });

        sender
    }

    #[tracing::instrument]
    pub async fn connect(addr: &str) -> anyhow::Result<(Self, UnboundedReceiver<Message>)> {
        let stream = TcpStream::connect(addr).await?;

        let mut framed_socket = Framed::new(stream, LengthDelimitedCodec::new());

        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        let (tx_receiver, rx_receiver) = mpsc::unbounded_channel::<Message>();

        let command_sender = Self::command_processor(tx.clone(), tx_receiver);

        let command_sender_copy = command_sender.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {

                    Some(Ok(bytes)) = framed_socket.next() => {
                        match Message::from_bytes(&bytes[..]) {
                            Ok(message) => {
                        let _ = command_sender.send(ClientCommand::MessageReceived(message));
                            },
                            Err(e) => {
                                error!(Error = format!("{:#?}", e), "An error occurred on creating a message from the received framed bytes");
                            }
                        }
                    }

                    Some(message) = rx.recv() => {
                        if let Err(e) =framed_socket.send(message.to_bytes().into()).await {
                                error!(Error = format!("{:#?}", e), "Failed to send a message to the server.");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_test() {
        if let Ok((client, _message_receiver)) = TaxicabClient::connect("127.0.0.1::1729").await {
            client.send("").await;
        }
    }
}
