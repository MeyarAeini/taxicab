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

///Keep the state of a taxicab client.
///
///A taxicab client send a `Message` to the taxicab server using tokio `TcpStream`.
///
///It also receives the `Message`s from the taxicab server to process them.
pub struct TaxicabClient {
    sender: UnboundedSender<ClientCommand>,
}

impl TaxicabClient {

    ///taxicab internal command processor
    ///
    ///it get two sender channeld for receiving and sending Message and returns the Command sender
    ///channel to receive commands from.
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

    ///It connect a taxicab client to the taxicab server by the given taxicab address.
    ///This method return a `Result` wrapped instance of taxicab client and a channel
    ///unbounded receiver instance of the `Message`s.
    ///
    ///This method is instrumented by the `tracing` crate so the client side code can initilize the
    ///tracing and subscribe to the tracing events.
    ///
    ///The client uses the `tokio-util` crate to transmit Framed messages. It uses the
    ///LengthDelimitedCodec.
    #[tracing::instrument]
    pub async fn connect(addr: &str) -> anyhow::Result<(Self, UnboundedReceiver<Message>)> {
        //initilize a TcpStream connected to the taxicab server
        let stream = TcpStream::connect(addr).await?;

        //Create a Framed I/O read/write stream to read/write the raw data in a `Message` chunck.
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

        //create a mpsc channel to be served for sending a message. The sender of this
        //channel will be kept in the taxicab client state and the receiver will be moved to the
        //tokio spawn waiting to get a message an transfer it to the taxicab server
        let (tx_sender, mut rx_sender) = mpsc::unbounded_channel::<Message>();

        //create a mpsc channel to be served for receiving a message.
        //The receiver of the channel will be returned by this method to give a handle to the client
        //code. The receiver of the channel will be used to receive the received messages from the
        //taxicab server.
        let (tx_receiver, rx_receiver) = mpsc::unbounded_channel::<Message>();

        //stablish the taxicab command processor
        let command_sender = Self::command_processor(tx_sender, tx_receiver);

        let taxicab_command_sender = command_sender.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {

                    //wait to receive a new message from the taxicab server
                    Some(Ok(bytes)) = transport.next() => {
                        match Message::from_bytes(&bytes[..]) {
                            Ok(message) => {
                        let _ = command_sender.send(ClientCommand::MessageReceived(message));
                            },
                            Err(e) => {
                                error!(Error = format!("{:#?}", e), "An error occurred on creating a message from the received framed bytes");
                            }
                        }
                    }

                    //wait to receive a new message to be sent to the taxicab server
                    Some(message) = rx_sender.recv() => {
                        if let Err(e) = transport.send(message.to_bytes().into()).await {
                                error!(Error = format!("{:#?}", e), "Failed to send a message to the server.");
                            }
                    }
                }
            }
        });

        Ok((
            Self {
                sender: taxicab_command_sender,
            },
            rx_receiver,
        ))
    }

    ///Send a string slice message to the taxicab server.
    ///
    ///This method uses the taxicab internal Command channel to pass the message to the taxicab tcp
    ///stream.
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
