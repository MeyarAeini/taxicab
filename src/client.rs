use std::{collections::HashSet, marker::PhantomData, net::SocketAddr, pin::Pin};

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::{
        Mutex,
        broadcast::Receiver,
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        watch::{self, Sender},
    },
    task::JoinHandle,
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

///The trait to be implemented for each message which we expected to receive a message for from the
///`taxicab` server
#[async_trait]
pub trait MessageHandler<'de> {
    ///The MessageHandler error type
    type Error: Into<Box<dyn Error>> + Send;

    ///The MessageHandler Message type
    type Message: Serialize + Deserialize<'de> + Send;

    ///A handler function which will be called by receiving each Message type
    async fn handle(
        &self,
        taxicab: &TaxicabClient<Driving>,
        message: Self::Message,
    ) -> Result<(), Self::Error>;
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
        taxicab: Arc<TaxicabClient<Driving>>,
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
        taxicab: Arc<TaxicabClient<Driving>>,
        message: serde_json::Value,
        _: private::Token,
    ) -> Result<(), Box<dyn Error>> {
        let typed_message: T = serde_json::from_value(message)?;
        self.handler
            .handle(&*taxicab, typed_message)
            .await
            .map_err(Into::into)
    }
}

///The message registry to hold the message handlers and routing paths mappings
//pub struct MessageHandlerRegistry {
//}

//impl MessageHandlerRegistry {
//    ///Initilize a `MessagHandlerRegistry` object
//    pub fn new() -> Self {
//        Self {
//            handlers: HashMap::new(),
//        }
//    }
//
//    ///add a message handler path mapping
//    pub fn insert<MH>(&mut self, path: MessagePath, handler: MH)
//    where
//        MH: DynamicMessageHandler + 'static,
//    {
//        self.handlers.insert(path, Box::new(handler));
//    }
//
//    fn get(&self, message_type: &MessagePath) -> Option<&Box<dyn DynamicMessageHandler>> {
//        self.handlers.get(message_type)
//    }
//}

///Represents the `taxicab` client not connected status, in this state only is possible to set the
///taxicab client up and can not sent or receive any messages to the server
pub struct EngineOff;

///Represents the `taxicab` client connected status, in this state you can sent and receive
///messages from the `taxicab` server
pub struct Driving;

#[derive(Clone)]
pub(crate) struct ShutdownData;

///The taxicab client, this object can initilize the connection with the taxicab server. Also it
///holds the client state such as message handler refrences and the cancellation tokens
pub struct TaxicabClient<State = EngineOff> {
    //address: SocketAddr,
    db: Db,
    sender: Option<UnboundedSender<Message>>,
    state: PhantomData<State>,
}

type MessageHandlerRegistry = HashMap<MessagePath, Box<dyn DynamicMessageHandler>>;

///The `taxicab` builder
///
///This will help to configure a taxicab client. It is implementing the [Builder](https://rust-lang.github.io/api-guidelines/type-safety.html?highlight=builder#consuming-builders) pattern
///
pub struct TaxicabBuilder {
    address: SocketAddr,
    handlers: MessageHandlerRegistry,
    tasks: Vec<DynamicTaxicabTask>,
    shutdown: Option<Pin<Box<dyn Future<Output = Option<()>>>>>,
}

impl TaxicabBuilder {
    ///Creates a new `TaxicabBuilder` with specifying the socket address of the taxicab server
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            handlers: HashMap::new(),
            tasks: Vec::new(),
            shutdown: None,
        }
    }

    ///Add a task to be ran after connecting to the `taxicab` server
    pub fn with_task<T>(mut self, task: T) -> Self
    where
        T: Fn(
                Arc<TaxicabClient<Driving>>,
            )
                -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send>>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.tasks.push(DynamicTaxicabTask {
            task: Box::new(task),
        });

        self
    }

    ///add a message handler path mapping
    pub fn with_handler<MH>(mut self, path: MessagePath, handler: MH) -> Self
    where
        MH: DynamicMessageHandler + 'static,
    {
        self.handlers.insert(path, Box::new(handler));

        self
    }

    ///Sepcify a shutdown signal to be wait for and shutdown all ongoing tasks as well as currently
    ///under processing messages
    pub fn shutdown_on(mut self, signal: Pin<Box<dyn Future<Output = Option<()>>>>) -> Self {
        self.shutdown = Some(Box::pin(signal));

        self
    }
}

struct DynamicTaxicabTask {
    task: Box<
        dyn Fn(
                Arc<TaxicabClient<Driving>>,
            )
                -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send>>> + Send>>
            + Send
            + Sync,
    >,
}

#[async_trait]
impl TaxicabTask for DynamicTaxicabTask {
    async fn run<C>(
        &mut self,
        taxicab: Arc<TaxicabClient<Driving>>,
        cancel: C,
    ) -> Result<(), Box<dyn Error + Send>>
    where
        C: Fn() -> Receiver<ShutdownData> + Send + Sync,
    {
        (self.task)(taxicab).await
    }
}

#[async_trait]
trait TaxicabTask: Send + Sync {
    async fn run<C>(
        &mut self,
        taxicab: Arc<TaxicabClient<Driving>>,
        cancel: C,
    ) -> Result<(), Box<dyn Error + Send>>
    where
        C: Fn() -> Receiver<ShutdownData> + Sync + Send;
}

struct TaxicabMessageReceiverTask {
    receiver: Option<UnboundedReceiver<Message>>,
}

impl TaxicabMessageReceiverTask {
    fn new(receiver: UnboundedReceiver<Message>) -> Self {
        Self {
            receiver: Some(receiver),
        }
    }
}

#[async_trait]
impl TaxicabTask for TaxicabMessageReceiverTask {
    async fn run<C>(
        &mut self,
        taxicab: Arc<TaxicabClient<Driving>>,
        cancel: C,
    ) -> Result<(), Box<dyn Error + Send>>
    where
        C: Fn() -> Receiver<ShutdownData> + Sync + Send,
    {
        //I guess here the db better to be an option so i would be able to take it out and consume
        //it here instead of cloning it
        //let db = taxicab.db.clone();

        //loop {
        if let Some(mut receiver) = self.receiver.take() {
            while let Some(message) = receiver.recv().await {
                match message {
                    Message::Request(message) => {
                        let (cancelation_sender, mut cancelation_receiver) = watch::channel(false);
                        let mut cancellation_signals =
                            taxicab.db.process_cancellations.lock().await;
                        cancellation_signals.insert(message.id().clone(), cancelation_sender);
                        drop(cancellation_signals);
                        match serde_json::from_str(&message.content) {
                            Ok(data) => {
                                let db = taxicab.db.clone();
                                let command_sender =
                                    taxicab.sender.clone().expect("sender must have value");
                                let message_id = message.id().clone();
                                let taxicab = taxicab.clone();

                                let mut shutdown_rx = cancel();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        Ok(_) = db.handler_registry.get(&message.path)
                                            .map(|handler| handler.handle(taxicab,data,private::Token{}))
                                            .expect("The message handler is not existing or not registered")=> {

                                            info!(message_id = message_id.to_string(), "message processed successfully and the acknowledge has sent to the taxicab server");
                                           let _ = command_sender.send(Message::Ack(message_id));
                                        }
                                        _ = cancelation_receiver.changed() => {
                                            warn!(message_id = message_id.to_string(), "The message timeouted and canceled by the server signal");
                                        }

                                        _ = shutdown_rx.recv() => {
                                            info!(message_id = message_id.to_string() ,"Received the shutdown signal, discarding all processes");
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
                        let mut cancellation_signals =
                            taxicab.db.process_cancellations.lock().await;
                        cancellation_signals
                            .remove(&message_id)
                            .map(|signal| signal.send(true));
                        info!(
                            message_id = message_id.to_string(),
                            "the message processing should be canceled. the cancelation signal is sent."
                        );
                    }
                    _ => {
                        unreachable!()
                    } //all other messages should not be received here
                }
            }
        }
        Ok(())
        //}
    }
}

//type TaxicabTask = Box<dyn Fn(&TaxicabClient<Driving>, impl Future) -> Pin<Box<dyn Future<Output = ()>+ Send> + Send + Sync>>;

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

//impl TaxicabClient<EngineOff> {
//    ///Initilize a `taxicab` client object with the given `taxicab` server socket address and the
//    ///message handlers registry
//    pub fn new(builder: TaxicabBuilder) -> TaxicabClient<EngineOff> {
//        Self {
//            db: Db::new(builder.handlers),
//            sender: None,
//            state: PhantomData,
//        }
//    }
//}

struct TaxicabTransport;

impl TaxicabTransport {
    async fn connect<F>(
        addr: SocketAddr,
        _on_receive: F,
    ) -> anyhow::Result<(UnboundedReceiver<Message>, UnboundedSender<Message>)>
    where
        F: FnOnce(UnboundedReceiver<Message>),
    {
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
        //let command_sender = Self::command_processor(tx_sender, tx_receiver);

        //let taxicab_command_sender = command_sender.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {

                    //wait to receive a new message from the taxicab server
                    Some(Ok(bytes)) = transport.next() => {
                        match Message::from_bytes(&bytes[..]) {
                            Ok(message) => {
                        let _ = tx_receiver.send(message);
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

        Ok((rx_receiver, tx_sender))
    }
}

impl TaxicabBuilder {
    ///It connect a taxicab client to the taxicab server by the given taxicab address.
    ///This method return a `Result` wrapped instance of taxicab client and a channel
    ///unbounded receiver instance of the `Message`s.
    ///
    ///This method is instrumented by the `tracing` crate so the client side code can initilize the
    ///tracing and subscribe to the tracing events.
    ///
    ///The client uses the `tokio-util` crate to transmit Framed messages. It uses the
    ///LengthDelimitedCodec.
    pub async fn connect(self, shutdown: impl Future) -> Result<(), Box<dyn Error>> {
        //TODO: the connect should get a closure for receiving a message and return the Self to be
        //kept in the taxicab client object
        let (rx, tx) = TaxicabTransport::connect(self.address, |_rx| {}).await?;

        let (tx_shutdown, _) = tokio::sync::broadcast::channel(16);
        Self::send_exchange_bindings_to_server(self.handlers.keys(), tx.clone());
        let client: Arc<TaxicabClient<Driving>> = Arc::new(TaxicabClient {
            db: Db::new(self.handlers),
            sender: Some(tx),
            state: PhantomData,
        });

        let mut tasks = Vec::new();

        let shutdown_receiver = tx_shutdown.clone();
        tasks.push(
            client
                .clone()
                .spawn(TaxicabMessageReceiverTask::new(rx), move || {
                    shutdown_receiver.subscribe()
                }),
        );

        for task in self.tasks {
            let shutdown = tx_shutdown.clone();
            tasks.push(client.clone().spawn(task, move || shutdown.subscribe()));
        }

        tokio::select! {
        _= futures::future::join_all(tasks) => {
            info!("the client tasks are finished");
        },
        _ = shutdown => {
        warn!("the shutdown signal is received");
        }
        }
        //let taxicab_remote = client.clone();

        warn!("The shutdown signal going to be broadcasted");

        let _ = tx_shutdown.send(ShutdownData);

        //TODO : put a mechanism in place to ensure the broadcasted signal are received and all
        //task and handlers are stoped working
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        drop(tx_shutdown);

        Ok(())
    }

    fn send_exchange_bindings_to_server<'p, P>(paths: P, command_sender: UnboundedSender<Message>)
    where
        P: Iterator<Item = &'p MessagePath>,
    {
        let mut sent = HashSet::new();

        for bindings in paths {
            //do not sent duplicated binding request
            if !sent.contains(&bindings.exchange) {
                sent.insert(&bindings.exchange);
                let _ = command_sender.send(Message::Binding(bindings.exchange.to_string()));
            }
        }
    }
}

impl TaxicabClient<Driving> {
    fn spawn<T, C>(self: Arc<Self>, mut task: T, cancel: C) -> JoinHandle<()>
    where
        T: TaxicabTask + Send + Sync + 'static,
        C: Fn() -> Receiver<ShutdownData> + Send + Sync + 'static,
    {
        let mut shutdown = cancel();
        tokio::spawn(async move {
            tokio::select! {
                result = task.run(self,cancel) => {
                   if result.is_err() {
                       error!(error = format!("{:#?}", result), "the task is going to stop, an error happend");
                   }
                }

                _ = shutdown.recv() => {
                    warn!("the task is going to stop, a shutdown signal received")
                }
            }
        })
    }
    // fn host_message_receiver(
    //     self: Arc<Self>,
    //     mut rx: UnboundedReceiver<Message>,
    // ) -> JoinHandle<anyhow::Result<()>> {
    //     let mut shutdown_rx = self
    //         .shutdown_tx
    //         .as_ref()
    //         .expect("The shutdown sender must have value")
    //         .subscribe();

    //     //I guess here the db better to be an option so i would be able to take it out and consume
    //     //it here instead of cloning it
    //     let db = self.db.clone();

    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 Some(message) = rx.recv() => {
    //                     match message {
    //                         Message::Request(message) => {
    //                             let (cancelation_sender, mut cancelation_receiver) = watch::channel(false);
    //                             let mut cancellation_signals = db.process_cancellations.lock().await;
    //                             cancellation_signals.insert(message.id().clone(), cancelation_sender);
    //                             drop(cancellation_signals);
    //                             match serde_json::from_str(&message.content) {
    //                                 Ok(data) => {
    //                                     let db = db.clone();
    //                                     let command_sender = self.sender.clone().expect("sender must have value");
    //                                     let message_id = message.id().clone();
    //                                     let taxicab = self.clone();

    //                                     let mut shutdown_rx = taxicab.shutdown_tx.as_ref().expect("The shutdown sender must have value").subscribe();
    //                                     tokio::spawn(async move {
    //                                         tokio::select! {
    //                                             Ok(_) = db.handler_registry.get(&message.path)
    //                                                 .map(|handler| handler.handle(taxicab,data,private::Token{}))
    //                                                 .expect("The message handler is not existing or not registered")=> {

    //                                                 info!(message_id = message_id.to_string(), "message processed successfully and the acknowledge has sent to the taxicab server");
    //                                                let _ = command_sender.send(Message::Ack(message_id));
    //                                             }
    //                                             _ = cancelation_receiver.changed() => {
    //                                                 warn!(message_id = message_id.to_string(), "The message timeouted and canceled by the server signal");
    //                                             }

    //                                             _ = shutdown_rx.recv() => {
    //                                                 info!(message_id = message_id.to_string() ,"Received the shutdown signal, discarding all processes");
    //                                             }
    //                                         }
    //                                     });
    //                                 }
    //                                 Err(e) => {
    //                                     error!(Error = format!("{:#?}", e));
    //                                 }
    //                             }
    //                         }
    //                         Message::Cancellation(message_id) => {
    //                             let mut cancellation_signals = db.process_cancellations.lock().await;
    //                             cancellation_signals
    //                                 .remove(&message_id)
    //                                 .map(|signal| signal.send(true));
    //                             info!(
    //                                 message_id = message_id.to_string(),
    //                                 "the message processing should be canceled. the cancelation signal is sent."
    //                             );
    //                         }
    //                         _ => {unreachable!()} //all other messages should not be received here
    //                     }
    //                 }

    //                 _ = shutdown_rx.recv() => {
    //                     info!("Received the shutdown signal, discarding all processes");
    //                     break;
    //                 }
    //             }
    //         }

    //         Ok(())
    //     })
    // }

    ///Send a string slice message to the taxicab server.
    ///
    ///This method uses the taxicab internal Command channel to pass the message to the taxicab tcp
    ///stream.
    pub async fn send<M: Serialize>(&self, message: &M, path: MessagePath) -> anyhow::Result<()> {
        let message = Message::new_command(path, serde_json::to_string(message)?);
        self.send_command(message)
    }

    fn send_command(&self, command: Message) -> anyhow::Result<()> {
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
