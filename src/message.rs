use bincode::{Decode, Encode, config, error::DecodeError};

#[derive(Encode, Decode, Debug)]
pub struct Message {
    pub(crate) header: MessageHeader,
    pub body: String,
}

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
    pub fn new(exchange: String, body: String) -> Self {
        let header = MessageHeader {
            exchange,
            ..Default::default()
        };

        Self { header, body }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let config = config::standard();

        let bytes = bincode::encode_to_vec(&self, config).unwrap();

        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let config = config::standard();
        bincode::decode_from_slice(bytes, config).map(|(message, _size)| message)
    }
}
