use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    usize,
};

use tokio::sync::{
    Notify,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio::time::{Duration, Instant};
use tracing::{debug, info};

use crate::{
    Message,
    message::{CommandMessage, MessageId},
};

type MessageSender = UnboundedSender<Message>;
//type MessageValue = Vec<u8>;
pub(crate) type DbEventListener = UnboundedReceiver<DbEvent>;
pub(crate) type DbExchangeListener = (String, DbEventListener);

#[derive(Debug)]
struct MessageEntry {
    value: CommandMessage,
    received: Option<Instant>,
    sent: Option<Instant>,
    ack: Option<Instant>,
    consumer: Option<String>,
}

//#[derive(Debug)]
//struct Chauffeur {
//    endpoint: String,
//}

#[derive(Debug, Clone)]
pub(crate) struct Db {
    shared: Arc<Shared>,
    event_sender: UnboundedSender<DbExchangeListener>,
    event_senders: HashMap<String, UnboundedSender<DbEvent>>,
}

pub(crate) enum DbEvent {
    NewMessage(String),
    NewBinding(String),
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,

    background_task: Notify,
}

#[derive(Debug)]
struct State {
    endpoints: HashMap<String, MessageSender>,
    exchanges: HashMap<String, VecDeque<MessageId>>,
    bindings: HashMap<String, HashSet<String>>,
    messages: HashMap<MessageId, MessageEntry>,
    //work in progress
    wip: BTreeSet<(Instant, MessageId)>,
    //endpoint work loads ,  (exchange, endpoint) -> current work load
    ewl: HashMap<String, HashMap<String, usize>>,
}

impl State {
    fn push_message(&mut self, message: CommandMessage) -> Option<&MessageEntry> {
        if self.messages.contains_key(message.id()) {
            return None;
        }

        let State {
            messages,
            exchanges,
            ..
        } = &mut *self;

        {
            //make sure the queue exist for the message exchage or create a new one
            if !exchanges.contains_key(message.exchange()) {
                exchanges.insert(message.exchange().to_string(), VecDeque::new());
            }
        }

        let mut message = MessageEntry::new(message);
        message.received = Some(Instant::now());

        //insert the message for its permanement storage
        let message = messages
            .entry(message.value.id().clone())
            .or_insert(message);

        //push the message id on the queue waiting to be delivered to a consumer
        exchanges
            .get_mut(message.value.exchange())
            .map(|queue| queue.push_back(message.value.id().clone()));

        Some(message)
    }

    fn has_exchange(&self, exchange: &str) -> bool {
        self.exchanges.contains_key(exchange)
    }

    fn next_chauffeur<'binding>(
        exchange: &str,
        bindings: &'binding HashMap<String, HashSet<String>>,
        ewl: &HashMap<String, HashMap<String, usize>>,
    ) -> Option<&'binding str> {
        if let Some(exchange_bindings) = bindings.get(exchange) {
            let mut result: Option<&'binding str> = None;
            let mut least_load = usize::MAX;
            for endpoint in exchange_bindings.iter().map(|endpoint| {
                let load = ewl
                    .get(exchange)
                    .and_then(|exchange_load| exchange_load.get(endpoint))
                    .unwrap_or(&0);

                (endpoint, load)
            }) {
                if endpoint.1 < &least_load {
                    least_load = endpoint.1.clone();
                    result = Some(endpoint.0.as_str());
                }
            }

            result
        } else {
            None
        }
    }

    fn add_binding(&mut self, exchange: &str, endpoint: &str) -> Option<&str> {
        {
            if !self.bindings.contains_key(exchange) {
                self.bindings.insert(exchange.to_string(), HashSet::new());
            }
        }

        if let Some(bindings) = self.bindings.get_mut(exchange) {
            if bindings.contains(endpoint) {
                return None;
            }

            bindings.insert(endpoint.to_string());

            return bindings.get(endpoint).map(|v| v.as_str());
        }

        None
    }
}

impl MessageEntry {
    fn new(value: CommandMessage) -> Self {
        Self {
            value,
            received: None,
            sent: None,
            ack: None,
            consumer: None,
        }
    }
}

impl Db {
    pub fn new() -> (Self, UnboundedReceiver<DbExchangeListener>) {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                endpoints: HashMap::new(),
                exchanges: HashMap::new(),
                bindings: HashMap::new(),
                messages: HashMap::new(),
                wip: BTreeSet::new(),
                ewl: HashMap::new(),
            }),
            background_task: Notify::new(),
        });

        tokio::spawn(purge_overdue_messages(shared.clone()));

        let (tx, rx) = mpsc::unbounded_channel::<DbExchangeListener>();

        (
            Self {
                shared,
                event_sender: tx,
                event_senders: HashMap::new(),
            },
            rx,
        )
    }

    pub fn instance(&self) -> Self {
        self.clone()
    }

    pub fn keep_endpoint_in_loop(&mut self, endpoint: &str, endpoint_sender: MessageSender) {
        info!(endpoint = endpoint, "keep a new endpoint in the loop");
        let mut state = self.shared.state.lock().unwrap();

        if !state.endpoints.contains_key(endpoint) {
            state
                .endpoints
                .insert(endpoint.to_string(), endpoint_sender);
        }
    }

    pub fn forward_message_to(&self, message: Message, to_endpoint: &str) -> anyhow::Result<()> {
        let state = self.shared.state.lock().unwrap();
        if let Some(endpoint) = state.endpoints.get(to_endpoint) {
            endpoint.send(message)?;
        }

        Ok(())
    }

    pub fn enqueue(&mut self, message: CommandMessage) {
        let state = &mut *self.shared.state.lock().unwrap();

        if !state.has_exchange(message.exchange()) {
            let (tx, rx) = mpsc::unbounded_channel::<DbEvent>();
            let _ = self.event_sender.send((message.exchange().to_string(), rx));
            self.event_senders
                .insert(message.exchange().to_string(), tx);
        }

        debug!(
            message_id = message.message_id(),
            "try to enqueue the message"
        );

        if let Some(message) = state.push_message(message) {
            //send out an event indicating a new message is received
            self.send_event(
                message.value.exchange(),
                DbEvent::NewMessage(message.value.exchange().to_string()),
            );

            info!(
                exchange = message.value.exchange(),
                "A new message enqueued"
            );
        }
    }

    pub fn dequeue<A, Xs>(&mut self, exchange: Xs, action: A) -> anyhow::Result<bool>
    where
        A: Fn(CommandMessage, &str) -> anyhow::Result<()>,
        Xs: AsRef<str>,
    {
        let mut state = self.shared.state.lock().unwrap();

        let state = &mut *state;

        let exchange: &str = exchange.as_ref();

        debug!(exchange = exchange, "try to dequeue");

        if let Some(queue) = state.exchanges.get_mut(exchange) {
            debug!(exchange = exchange, "the excahnge exists");
            if let Some(endpoint) = State::next_chauffeur(exchange, &state.bindings, &state.ewl) {
                debug!(
                    exchange = exchange,
                    endpoint = endpoint,
                    "there is an endpoint to process a message"
                );
                let message = queue
                    .pop_front()
                    .and_then(|id| state.messages.get_mut(&id).map(|message| message));

                if let Some(message) = message {
                    debug!(
                        exchange = exchange,
                        endpoint = endpoint,
                        "going to process a message"
                    );
                    match action(message.value.clone(), endpoint) {
                        Ok(_) => {
                            //set the message sent time and the consumer value.
                            message.sent = Some(Instant::now());
                            message.consumer = Some(endpoint.to_string());

                            let overdue = state
                                .wip
                                .iter()
                                .next()
                                .map(|entry| entry.0.elapsed())
                                .is_some_and(|elapsed| elapsed > Duration::from_secs(2));
                            if overdue {
                                self.shared.background_task.notify_one();
                            }

                            //the message is added to the `Work In Progress` BTree set.
                            state
                                .wip
                                .insert((Instant::now(), message.value.id().clone()));

                            //mark the endpoint as an endpoint on process for the `exchange`
                            let endpoint_works = state
                                .ewl
                                .entry(exchange.to_string())
                                .or_insert(HashMap::new())
                                .entry(endpoint.to_string())
                                .or_insert(0);
                            *endpoint_works += 1;
                            debug!(
                                Count = *endpoint_works,
                                endpoint = endpoint,
                                exchange = exchange,
                                "Work load"
                            );
                        }
                        Err(e) => {
                            //something went wrong, push back the message into the queue
                            //any other action on data should be rolled back here
                            queue.push_front(message.value.id().clone());
                            return Err(e);
                        }
                    }

                    return Ok(true);
                }
            }
        }

        debug!(exchange = exchange, "exiting the dequeue process");

        Ok(false)
    }

    pub fn ack<Id, Cs>(&mut self, id: Id, endpoint: Cs) -> anyhow::Result<()>
    where
        Id: AsRef<MessageId>,
        Cs: AsRef<str>,
    {
        let State { messages, ewl, .. } = &mut *self.shared.state.lock().unwrap();

        let endpoint = endpoint.as_ref();

        match messages.get_mut(id.as_ref()) {
            Some(message) => {
                if message.consumer.as_deref() == Some(endpoint) {
                    //set the acknowledge time for the message
                    message.ack = Some(Instant::now());

                    if let Some(work_load) = ewl
                        .get_mut(message.value.exchange())
                        .and_then(|exchange_load| exchange_load.get_mut(endpoint))
                    {
                        *work_load -= 1;
                    }

                    debug!(
                        id = message.value.id().to_string(),
                        "A message processed successfully"
                    );
                }
            }
            None => {}
        }

        Ok(())
    }

    pub fn bind<Xs, Cs>(&mut self, exchange: Xs, endpoint: Cs)
    where
        Xs: AsRef<str>,
        Cs: AsRef<str>,
    {
        let mut state = self.shared.state.lock().unwrap();

        let exchange: &str = exchange.as_ref();
        if let Some(endpoint) = state.add_binding(exchange, endpoint.as_ref()) {
            self.send_event(exchange, DbEvent::NewBinding(exchange.to_string()));

            info!(
                exchange = exchange,
                endpoint = endpoint,
                "A new binding is added"
            );
        }
    }

    fn send_event(&self, exchange: &str, event: DbEvent) {
        if let Some(event_sender) = self.event_senders.get(exchange) {
            let _ = event_sender.send(event);
        }
    }
}

impl Shared {
    fn purge_overdue_messages(&self) -> Option<Instant> {
        let State {
            wip,
            messages,
            exchanges,
            endpoints,
            ..
        } = &mut *self.state.lock().unwrap();

        let due = Instant::now().checked_sub(Duration::from_secs(15)).unwrap();

        while let Some(&(when, ref message_id)) = wip.iter().next() {
            if when > due {
                return Some(when);
            }

            debug!(
                message_id = message_id.to_string(),
                "this message is overdue"
            );

            let is_processed = messages
                .get(&message_id)
                .is_some_and(|message| message.ack.is_some());

            //if the message's process is finished then just remove the message id from wip
            //otherwise move back the message to the queue
            if !is_processed {
                //update the message in the messages permanent storage
                if let Some(message) = messages.get_mut(&message_id) {
                    debug!("marking the message as not work in progress");
                    message.sent = None;
                    if let Some(endpoint) = message.consumer.take() {
                        //notify the current consumer about this decision
                        if let Some(endpoint) = endpoints.get(&endpoint) {
                            let _ = endpoint.send(Message::Cancellation(message_id.clone()));
                        }
                    }

                    if let Some(exchage) = exchanges.get_mut(message.value.exchange()) {
                        //enqueue the message id for next in front of the queue
                        exchage.push_front(message_id.clone());
                    }
                }
            }

            //remove the record from the wip
            wip.remove(&(when, message_id.clone()));
        }

        None
    }
}

async fn purge_overdue_messages(shared: Arc<Shared>) {
    loop {
        if let Some(when) = shared.purge_overdue_messages() {
            tokio::select! {
            _ = tokio::time::sleep_until(when) => {},
            _ = shared.background_task.notified() => {}
            }
        } else {
            //wait for the next background task notification
            shared.background_task.notified().await;
        }
    }
}
