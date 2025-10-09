use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;
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
    state: Arc<Mutex<State>>,
    event_sender: UnboundedSender<DbExchangeListener>,
    event_senders: HashMap<String, UnboundedSender<DbEvent>>,
}

pub(crate) enum DbEvent {
    NewMessage(String),
    NewBinding(String),
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
    ewl: BTreeMap<(String, String), usize>,
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
        let (tx, rx) = mpsc::unbounded_channel::<DbExchangeListener>();
        (
            Self {
                state: Arc::new(Mutex::new(State {
                    endpoints: HashMap::new(),
                    exchanges: HashMap::new(),
                    bindings: HashMap::new(),
                    messages: HashMap::new(),
                    wip: BTreeSet::new(),
                    ewl: BTreeMap::new(),
                })),
                event_sender: tx,
                event_senders: HashMap::new(),
            },
            rx,
        )
    }

    pub fn instance(&self) -> Self {
        self.clone()
    }

    pub fn keep_endpoint_in_loop(&mut self, endpoint: String, endpoint_sender: MessageSender) {
        info!(endpoint = endpoint, "keep a new endpoint in the loop");
        let mut state = self.state.lock().unwrap();

        state.endpoints.insert(endpoint.clone(), endpoint_sender);
    }

    pub fn forward_message_to(&self, message: Message, to_endpoint: &str) -> anyhow::Result<()> {
        let state = self.state.lock().unwrap();
        if let Some(endpoint) = state.endpoints.get(to_endpoint) {
            endpoint.send(message)?;
        }

        Ok(())
    }

    pub fn enqueue(&mut self, message: CommandMessage) {
        let mut state = self.state.lock().unwrap();

        let State {
            exchanges,
            messages,
            ..
        } = &mut *state;

        if !exchanges.contains_key(message.exchange()) {
            let (tx, rx) = mpsc::unbounded_channel::<DbEvent>();
            let _ = self.event_sender.send((message.exchange().to_string(), rx));
            self.event_senders
                .insert(message.exchange().to_string(), tx);
        }

        let message_id = message.id().clone();
        let message_trace = message.message_id().to_string();

        debug!(message_id = message_trace, "enqueue");

        if !messages.contains_key(&message_id) {
            //insert the message for its permanement storage
            let message = messages.entry(message_id.clone()).or_insert({
                let mut message = MessageEntry::new(message);
                message.received = Some(Instant::now());
                message
            });

            //make sure the queue exist for the message exchage or create a new one
            let queue = exchanges
                .entry(message.value.exchange().to_string())
                .or_insert(VecDeque::new());

            //push the message id on the queue waiting to be delivered to a consumer
            queue.push_back(message_id);

            //send out an event indicating a new message is received
            self.send_event(
                message.value.exchange(),
                DbEvent::NewMessage(message.value.exchange().to_string()),
            );

            info!(
                exchange = message.value.exchange(),
                queued_count = queue.len(),
                "A new message enqueued"
            );
        }

        debug!(message_id = message_trace, "enqueue exit");
    }

    pub fn dequeue<A>(&mut self, exchange: &str, action: A) -> anyhow::Result<bool>
    where
        A: Fn(CommandMessage, String) -> anyhow::Result<()>,
    {
        let mut state = self.state.lock().unwrap();

        let State {
            exchanges,
            messages,
            bindings,
            wip,
            ewl,
            ..
        } = &mut *state;

        debug!(exchange = exchange, "try to dequeue");

        if let Some(queue) = exchanges.get_mut(exchange) {
            debug!(exchange = exchange, "the excahnge exists");
            if let Some(endpoint) = bindings
                .get(exchange)
                .map(|set| set.clone().into_iter())
                .into_iter()
                .flatten()
                .next()
            {
                debug!(
                    exchange = exchange,
                    endpoint = endpoint,
                    "there is an endpoint to process a message"
                );
                let message = queue
                    .pop_front()
                    .and_then(|id| messages.get_mut(&id).map(|message| message));

                if let Some(message) = message {
                    debug!(
                        exchange = exchange,
                        endpoint = endpoint,
                        "going to process a message"
                    );
                    match action(message.value.clone(), endpoint.clone()) {
                        Ok(_) => {
                            //set the message sent time and the consumer value.
                            message.sent = Some(Instant::now());
                            message.consumer = Some(endpoint.clone());

                            //the message is added to the `Work In Progress` BTree set.
                            wip.insert((Instant::now(), message.value.id().clone()));

                            //mark the endpoint as an endpoint on process for the `exchange`
                            let endpoint_works = ewl
                                .entry((exchange.to_string(), endpoint.clone()))
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

    pub fn ack(&mut self, id: MessageId, endpoint: &str) -> anyhow::Result<()> {
        let State { messages, .. } = &mut *self.state.lock().unwrap();

        let trace_id = id.to_string();
        messages.entry(id).and_modify(|message| {
            if message.consumer.as_deref() == Some(endpoint) {
                message.ack = Some(Instant::now());
                debug!(
                    id = trace_id,
                    message = format!("{:#?}", message),
                    "A message processed successfully"
                );
            }
        });

        Ok(())
    }

    // pub fn exchange_count(&self, exchange: &str) -> usize {
    //     let state = self.state.lock().unwrap();

    //     if let Some(exchange) = state.exchanges.get(exchange) {
    //         exchange.len()
    //     } else {
    //         0
    //     }
    // }

    pub fn bind(&mut self, exchange: &str, endpoint: String) {
        let mut state = self.state.lock().unwrap();

        let entry = state.bindings.entry(exchange.to_string());

        let binding = entry.or_insert(HashSet::new());

        binding.insert(endpoint.to_string());

        self.send_event(exchange, DbEvent::NewBinding(exchange.to_string()));

        info!(
            exchange = exchange,
            endpoint = &endpoint,
            "A new binding is added"
        );
    }

    //pub fn who_handles(&self, exchange: &str) -> impl Iterator<Item = String> {
    //    let state = self.state.lock().unwrap();

    //    state
    //        .bindings
    //        .get(exchange)
    //        .map(|set| set.clone().into_iter())
    //        .into_iter()
    //        .flatten()
    //}

    fn send_event(&self, exchange: &str, event: DbEvent) {
        if let Some(event_sender) = self.event_senders.get(exchange) {
            let _ = event_sender.send(event);
        }
    }
}
