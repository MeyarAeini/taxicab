use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc::UnboundedSender;
use tracing::info;

use crate::Message;

type MessageSender = UnboundedSender<Message>;
type MessageValue = Vec<u8>;

#[derive(Debug, Clone)]
pub(crate) struct Db {
    state: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
    endpoints: HashMap<String, MessageSender>,
    exchanges: HashMap<String, VecDeque<MessageValue>>,
    bindings: HashMap<String, HashSet<String>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                endpoints: HashMap::new(),
                exchanges: HashMap::new(),
                bindings: HashMap::new(),
            })),
        }
    }

    pub fn instance(&self) -> Self {
        self.clone()
    }

    pub fn keep_endpoint_in_loop(&mut self, endpoint: String, endpoint_sender: MessageSender) {
        info!(endpoint = endpoint, "keep a new endpoint in the loop");
        let mut state = self.state.lock().unwrap();

        state.endpoints.insert(endpoint, endpoint_sender);
    }

    pub fn forward_message_to(&self, message: Message, to_endpoint: &str) -> anyhow::Result<()> {
        let state = self.state.lock().unwrap();
        if let Some(endpoint) = state.endpoints.get(to_endpoint) {
            endpoint.send(message)?;
        }

        Ok(())
    }

    pub fn enqueue(&mut self, exchange: &str, message: MessageValue) {
        let mut state = self.state.lock().unwrap();

        let queue = state
            .exchanges
            .entry(exchange.to_string())
            .or_insert(VecDeque::new());

        queue.push_back(message);

        info!(
            exchange = exchange,
            queued_count = queue.len(),
            "A new message enqueued"
        );
    }

    pub fn bind(&mut self, exchange: &str, endpoint: String) {
        let mut state = self.state.lock().unwrap();

        let binding = state
            .bindings
            .entry(exchange.to_string())
            .or_insert(HashSet::new());

        binding.insert(endpoint.to_string());

        info!(
            exchange = exchange,
            endpoint = &endpoint,
            "A new binding is added"
        );
    }

    pub fn who_handles(&self, exchange: &str) -> impl Iterator<Item = String> {
        let state = self.state.lock().unwrap();

        state
            .bindings
            .get(exchange)
            .map(|set| set.clone().into_iter())
            .into_iter()
            .flatten()
    }
}
