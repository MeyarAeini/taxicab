use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{debug, error};

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
        match db.instance().dequeue(exchange, |message, endpoint| {
            let _ = controller.send_message(Message::Request(message), endpoint.to_string())?;
            debug!(
                exchange = exchange,
                endpoint = endpoint,
                "A new messsage sent to destination"
            );

            Ok(())
        }) {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
                error!(error = e.to_string());
            }
        }
    }
}
