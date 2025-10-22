use std::{collections::HashSet, marker::PhantomData, net::SocketAddr};

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::{
        Mutex,
        mpsc::{self, UnboundedSender},
        watch::{self, Sender},
    },
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{
    Message,
    message::{MessageId, MessagePath},
};
use tracing::{debug, error, info, warn};

use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

enum ClientCommand {
    MessageReceived(Message),
    SendMessage(Message),
}

///The trait to be implemented for each message which we expected to receive a message for from the
///`taxicab` server
#[async_trait]
pub trait MessageHandler<'de> {
    ///The MessageHandler error type
    type Error: Into<Box<dyn Error>> + Send;

    ///The MessageHandler Message type
    type Message: Serialize + Deserialize<'de> + Send;

    ///A handler function which will be called by receiving each Message type
    async fn handle(&self, message: Self::Message) -> Result<(), Self::Error>;
}

///For sealing the `DynamicMessageHandler`
/// - This will prevent the user of the crate to implement this trait
/// - Also it prevent the user of the crate to call its methods
mod private {
    pub trait Sealed {}
    pub struct Token;
}

#[async_trait]
pub trait DynamicMessageHandler: Send + Sync + private::Sealed {
    async fn handle(
        &self,
        message: serde_json::Value,
        _: private::Token,
    ) -> Result<(), Box<dyn Error>>;
}

///An adapter struct to register a MessageHandler dynamiclly
pub struct MessageHandlerAdapter<'de, T, MH>
where
    T: Serialize + Deserialize<'de> + Send,
    MH: MessageHandler<'de> + Send + Sync,
{
    handler: MH,
    phantom: PhantomData<&'de T>,
}

impl<'de, T, MH> MessageHandlerAdapter<'de, T, MH>
where
    T: Serialize + Deserialize<'de> + Send,
    MH: MessageHandler<'de, Message = T> + Send + Sync,
{
    ///Creates a new instance for the `MessageHandlerAdapter` for a given handler
    pub fn new(handler: MH) -> Self {
        Self {
            handler,
            phantom: PhantomData,
        }
    }
}

impl<'de, T, MH> private::Sealed for MessageHandlerAdapter<'de, T, MH>
where
    T: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    MH: MessageHandler<'de, Message = T> + Send + Sync,
{
}

#[async_trait]
impl<'de, T, MH> DynamicMessageHandler for MessageHandlerAdapter<'de, T, MH>
where
    T: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    MH: MessageHandler<'de, Message = T> + Send + Sync,
{
    async fn handle(
        &self,
        message: serde_json::Value,
        _: private::Token,
    ) -> Result<(), Box<dyn Error>> {
        let typed_message: T = serde_json::from_value(message)?;
        self.handler.handle(typed_message).await.map_err(Into::into)
    }
}

///The message registry to hold the message handlers and routing paths mappings
pub struct MessageHandlerRegistry {
    handlers: HashMap<MessagePath, Box<dyn DynamicMessageHandler>>,
}

impl MessageHandlerRegistry {
    ///Initilize a `MessagHandlerRegistry` object
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    ///add a message handler path mapping
    pub fn insert<MH>(&mut self, path: MessagePath, handler: MH)
    where
        MH: DynamicMessageHandler + 'static,
    {
        self.handlers.insert(path, Box::new(handler));
    }

    fn get(&self, message_type: &MessagePath) -> Option<&Box<dyn DynamicMessageHandler>> {
        self.handlers.get(message_type)
    }
}

///Represents the `taxicab` client not connected status, in this state only is possible to set the
///taxicab client up and can not sent or receive any messages to the server
pub struct EngineOff;

///Represents the `taxicab` client connected status, in this state you can sent and receive
///messages from the `taxicab` server
pub struct Driving;

///The taxicab client, this object can initilize the connection with the taxicab server. Also it
///holds the client state such as message handler refrences and the cancellation tokens
pub struct TaxicabClient<State = EngineOff> {
    address: SocketAddr,
    db: Db,
    sender: Option<UnboundedSender<ClientCommand>>,
    state: PhantomData<State>,
}

#[derive(Clone)]
struct Db {
    handler_registry: Arc<MessageHandlerRegistry>,
    process_cancellations: Arc<Mutex<HashMap<MessageId, Sender<bool>>>>,
}

impl Db {
    fn new(registry: MessageHandlerRegistry) -> Self {
        Self {
            handler_registry: Arc::new(registry),
            process_cancellations: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl TaxicabClient<EngineOff> {
    ///Initilize a `taxicab` client object with the given `taxicab` server socket address and the
    ///message handlers registry
    pub fn new(addr: SocketAddr, registry: MessageHandlerRegistry) -> TaxicabClient<EngineOff> {
        Self {
            address: addr,
            db: Db::new(registry),
            sender: None,
            state: PhantomData,
        }
    }
}

impl TaxicabClient<EngineOff> {
    ///taxicab internal command processor
    ///
    ///It take one sender channel for sending message and another sender channel for dispatching
    ///the received messages.
    ///It returns the Command sender channel to receive commands from.
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
                        //debug!(Message = message, " Sending a message to the server");
                        let _ = message_sender.send(message);
                    }
                    ClientCommand::MessageReceived(message) => {
                        debug!(
                            //   Message = message,
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
    pub async fn connect(self) -> anyhow::Result<TaxicabClient<Driving>> {
        //initilize a TcpStream connected to the taxicab server
        let stream = TcpStream::connect(self.address).await?;

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
        let (tx_receiver, mut rx_receiver) = mpsc::unbounded_channel::<Message>();

        //stablish the taxicab command processor
        let command_sender = Self::command_processor(tx_sender, tx_receiver);

        self.send_exchange_bindings_to_server(command_sender.clone());

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

        //I guess here the db better to be an option so i would be able to take it out and consume
        //it here instead of cloning it
        let db = self.db.clone();
        let internal_command_sender = taxicab_command_sender.clone();
        tokio::spawn(async move {
            while let Some(message) = rx_receiver.recv().await {
                match message {
                    Message::Request(message) => {
                        let (cancelation_sender, mut cancelation_receiver) = watch::channel(false);
                        let mut cancellation_signals = db.process_cancellations.lock().await;
                        cancellation_signals.insert(message.id().clone(), cancelation_sender);
                        drop(cancellation_signals);
                        match serde_json::from_str(&message.content) {
                            Ok(data) => {
                                let db = db.clone();
                                let command_sender = internal_command_sender.clone();
                                let message_id = message.id().clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        Ok(_) = db.handler_registry.get(&message.path)
                                            .map(|handler| handler.handle(data,private::Token{}))
                                            .expect("The message handler is not existing or not registered")=> {

                                            info!(message_id = message_id.to_string(), "message processed successfully and the acknowledge has sent to the taxicab server");
                                           let _ = command_sender.send(ClientCommand::SendMessage(Message::Ack(message_id)));
                                        }
                                        _ = cancelation_receiver.changed() => {
                                            warn!(message_id = message_id.to_string(), "The message timeouted and canceled by the server signal");
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                error!(Error = format!("{:#?}", e));
                            }
                        }
                    }
                    Message::Cancellation(message_id) => {
                        let mut cancellation_signals = db.process_cancellations.lock().await;
                        cancellation_signals
                            .remove(&message_id)
                            .map(|signal| signal.send(true));
                        info!(
                            message_id = message_id.to_string(),
                            "the message processing should be canceled. the cancelation signal is sent."
                        );
                    }
                    _ => {}
                }
            }
        });

        Ok(TaxicabClient {
            db: self.db,
            address: self.address,
            sender: Some(taxicab_command_sender),
            state: PhantomData,
        })
    }

    fn send_exchange_bindings_to_server(&self, command_sender: UnboundedSender<ClientCommand>) {
        let mut sent = HashSet::new();

        for bindings in self.db.handler_registry.clone().handlers.keys() {
            //do not sent duplicated binding request
            if !sent.contains(&bindings.exchange) {
                sent.insert(&bindings.exchange);
                let _ = command_sender.send(ClientCommand::SendMessage(Message::Binding(
                    bindings.exchange.to_string(),
                )));
            }
        }
    }
}

impl TaxicabClient<Driving> {
    ///Send a string slice message to the taxicab server.
    ///
    ///This method uses the taxicab internal Command channel to pass the message to the taxicab tcp
    ///stream.
    pub async fn send(&self, message: &str, path: MessagePath) -> anyhow::Result<()> {
        let message = Message::new_command(path, message.to_string());
        self.send_command(ClientCommand::SendMessage(message))
    }

    fn send_command(&self, command: ClientCommand) -> anyhow::Result<()> {
        self.sender
            .clone()
            .expect("taxicab in driving state must have the sender value")
            .send(command)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //use super::*;

    #[tokio::test]
    async fn client_test() {
        //if let Ok((client, _message_receiver)) = TaxicabClient::connect("127.0.0.1::1729").await {
        //    let _ = client.send("", "some_exchange").await;
        //}
    }
}
