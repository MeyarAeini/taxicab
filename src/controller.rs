use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{debug, error};

use crate::{
    Message,
    state::{Db, DbEvent, DbEventListener},
};

pub(crate) struct Controller;

pub enum Command {
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

#[derive(Clone)]
pub(crate) struct ControllerListener {
    sender: UnboundedSender<Command>,
}

impl ControllerListener {
    fn new(sender: UnboundedSender<Command>) -> Self {
        Self { sender }
    }

    pub(crate) fn new_client(
        &self,
        addr: String,
        sender: UnboundedSender<Message>,
    ) -> anyhow::Result<()> {
        self.sender.send(Command::NewClient { addr, sender })?;

        Ok(())
    }

    pub(crate) fn message_received(
        &self,
        message: Vec<u8>,
        from_addr: String,
    ) -> anyhow::Result<()> {
        self.sender
            .send(Command::MessageReceived { message, from_addr })?;

        Ok(())
    }

    pub(crate) fn send_message(&self, message: Message, to_addr: String) -> anyhow::Result<()> {
        self.sender
            .send(Command::SendMessage { message, to_addr })?;

        Ok(())
    }
}

impl Controller {
    async fn run() -> anyhow::Result<(Self, ControllerListener)> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Command>();

        let (db, db_event_listener) = Db::new();

        let controller_listener = ControllerListener::new(tx);

        let mut dispatcher = Dispatcher::new(db_event_listener, controller_listener.clone());

        let dispatcher_db = db.instance();
        tokio::spawn(async move {
            dispatcher.run(dispatcher_db).await;
        });

        let mut db = db.instance();
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    Command::NewClient { addr, sender } => {
                        debug!(Address = addr, "A new client is connected to the server");
                        db.keep_endpoint_in_loop(addr.to_string(), sender);
                    }

                    Command::SendMessage { message, to_addr } => {
                        debug!(Address = to_addr, "Dispatching a message to a client");
                        if let Err(_e) = db.forward_message_to(message, &to_addr) {
                            //error log;
                        }
                    }

                    Command::MessageReceived { message, from_addr } => {
                        Self::on_message_received(message, from_addr, &mut db);
                    }
                }
            }
        });

        Ok((Self {}, controller_listener.clone()))
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
}

pub async fn run_controller() -> anyhow::Result<ControllerListener> {
    Ok(Controller::run().await?.1)
}

struct Dispatcher {
    listener: UnboundedReceiver<(String, DbEventListener)>,
    controller: ControllerListener,
}

impl Dispatcher {
    pub(crate) fn new(
        listener: UnboundedReceiver<(String, UnboundedReceiver<DbEvent>)>,
        controller: ControllerListener,
    ) -> Self {
        Self {
            listener,
            controller,
        }
    }

    pub async fn run(&mut self, db: Db) {
        while let Some((exchange, listener)) = self.listener.recv().await {
            debug!(exchange = exchange, "Message dispatcher started for");

            self.spawn_exchange_dispatcher(db.instance(), listener);
        }
    }

    fn spawn_exchange_dispatcher(&self, db: Db, mut listener: UnboundedReceiver<DbEvent>) {
        let controller = self.controller.clone();
        let mut db = db.instance();
        tokio::spawn(async move {
            while let Some(event) = listener.recv().await {
                match event {
                    DbEvent::NewMessage(exchange) => {
                        let client = db.who_handles(&exchange).next();
                        if let Some(client) = client {
                            while let Some(message) = db.dequeue(&exchange) {
                                let _ = controller.send_message(
                                    Message::from_bytes(&message).unwrap(),
                                    client.to_string(),
                                );
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
