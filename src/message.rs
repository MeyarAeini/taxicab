use std::{fmt, str::FromStr, string::ParseError};

use bincode::{Decode, Encode, config, error::DecodeError};
use uuid::Uuid;

///Carries a message from client to server vice versa
///
///It has header information and a body which is String for now.
///Header has basic messaging information such as an identifier, a correlation identifier to trace a
///message subsequent dispatched messages. It also has a reply_to value to keep the identifier of
///requested message. All this values are optional and are not required. The header also has a
///required value by nature is the `exchange` value which defines the queue that the message will
///be stored into before being consumed by a client.
///
///For serilization of a `Message` the bincode is used since it is on the TCP layer and it is more
///perfomant than serde.
///
#[derive(Encode, Decode, Debug)]
pub enum Message {
    ///Binding
    ///
    ///A binding message is send from the client to server indicating `I will be waiting to receive
    ///messages of this exchange to handle them, you can count on me`
    Binding(String),

    ///Request/Command
    ///
    ///A request or a command is a kind of message client sending to server to be forwarded to a
    ///command handler client
    Request(CommandMessage),

    ///Acknowledge
    ///
    ///An acknowledge is a response to the server from a client indicating I handled the message
    Ack(MessageId),
}

#[derive(Debug, Encode, Decode, Clone)]
pub struct CommandMessage {
    message_id: MessageId,
    content: String,
    exchange: String,
}

impl CommandMessage {
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn message_id(&self) -> &str {
        &self.message_id.0
    }

    pub fn id(&self) -> &MessageId {
        &self.message_id
    }
}

#[derive(Encode, Decode, Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub struct MessageId(String);

impl MessageId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl FromStr for MessageId {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(MessageId(s.to_string()))
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Message {
    ///Initiate a new `Message` instance by the given `exchange` and the message `body` value.
    pub fn new_command(exchange: String, content: String) -> Self {
        Message::Request(CommandMessage {
            message_id: MessageId::new(),
            content,
            exchange,
        })
    }

    ///Initiate a new `Binding` message for a given `exchange`
    pub fn new_binding(exchange: String) -> Self {
        Message::Binding(exchange)
    }

    ///serialize a message to a vector of bytes.
    ///
    ///It uses the `bincode` crate for this purpose.
    pub fn to_bytes(&self) -> Vec<u8> {
        let config = config::standard();

        let bytes = bincode::encode_to_vec(&self, config).unwrap();

        bytes
    }

    ///deserialize a byte slice into a message.
    ///
    ///It uses the `bincode` crate for this purpose.
    ///The output is wraped into a Result to return the decode error in case of failure.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let config = config::standard();
        bincode::decode_from_slice(bytes, config).map(|(message, _size)| message)
    }
}
