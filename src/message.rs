use bincode::{Decode, Encode, config, error::DecodeError};

///Carries a message from client to server vice versa
///
///It has header information and a body which is String for now.
///The body value type might be updated since it will carry any types.
///
///For serilization of a `Message` the bincode is used since it is on the TCP layer and it is more
///perfomant than serde.
///
#[derive(Encode, Decode, Debug)]
pub struct Message {
    pub(crate) header: MessageHeader,

    ///carries the message body. The message body could be a simple message or a serialized value
    ///of a comlex type using serialization crate such as `serde`.
    pub body: String,
}

///It hold the header information for a `Message`.
///
///It has basic messaging information such as an identifier, a correlation identifier to trace a
///message subsequent dispatched messages. It also has a reply_to value to keep the identifier of
///requested message. All this values are optional and are not required. The header also has a
///required value by nature is the `exchange` value which defines the queue that the message will
///be stored into before being consumed by a client.
#[derive(Encode, Decode, Debug)]
pub struct MessageHeader {
    message_id: Option<String>,
    correlation_id: Option<String>,
    reply_to: Option<String>,
    pub(crate) exchange: String,
}

impl Default for MessageHeader {
    fn default() -> Self {
        Self {
            message_id: None,
            correlation_id: None,
            reply_to: None,
            exchange: String::new(),
        }
    }
}

impl Message {
    ///Initiate a new `Message` instance by the given `exchange` and the message `body` value.
    pub fn new(exchange: String, body: String) -> Self {
        let header = MessageHeader {
            exchange,
            ..Default::default()
        };

        Self { header, body }
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
