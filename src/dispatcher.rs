use tokio::sync::mpsc::UnboundedReceiver;
use tracing::debug;

use crate::{
    Message,
    controller::ControllerListener,
    state::{Db, DbEvent, DbEventListener},
};

pub struct Dispatcher {
    db_event_receiver: UnboundedReceiver<(String, DbEventListener)>,
    controller: ControllerListener,
}

impl Dispatcher {
    pub(crate) fn new(
        db_event_receiver: UnboundedReceiver<(String, DbEventListener)>,
        controller: ControllerListener,
    ) -> Self {
        Self {
            db_event_receiver,
            controller,
        }
    }

    pub async fn run(&mut self, db: Db) {
        while let Some((exchange, exchange_event_receiver)) = self.db_event_receiver.recv().await {
            debug!(exchange = exchange, "Message dispatcher started for");

            self.spawn_exchange_dispatcher(db.instance(), exchange_event_receiver);
        }
    }

    fn spawn_exchange_dispatcher(
        &self,
        db: Db,
        mut exchange_event_receiver: UnboundedReceiver<DbEvent>,
    ) {
        let controller = self.controller.clone();
        tokio::spawn(async move {
            while let Some(event) = exchange_event_receiver.recv().await {
                match event {
                    DbEvent::NewMessage(exchange) => {
                        process_next_message(&exchange, db.clone(), controller.clone());
                    }

                    DbEvent::NewBinding(exchange) => {
                        debug!(exchange = exchange, "A new binding added");
                        process_next_message(&exchange, db.clone(), controller.clone());
                    }
                }
            }
        });
    }
}

fn process_next_message(exchange: &str, db: Db, controller: ControllerListener) {
    loop {
        if db.exchange_count(exchange) == 0 {
            break;
        }

        match db.who_handles(exchange).next() {
            Some(client) => match db.instance().dequeue(exchange) {
                Some(message) => {
                    let _ = controller
                        .send_message(Message::from_bytes(&message).unwrap(), client.to_string());
                    debug!(
                        exchange = exchange,
                        clients = client,
                        "A new messsage sent to destination"
                    );
                }
                None => break,
            },
            None => break,
        }
    }
}
