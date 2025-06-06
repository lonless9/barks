//! Network server implementations

use crate::protocol::*;
use crate::traits::*;
use anyhow::Result;
use async_trait::async_trait;
use std::net::SocketAddr;

/// Basic message handler implementation
pub struct BasicMessageHandler;

#[async_trait]
impl MessageHandler<MessageEnvelope> for BasicMessageHandler {
    async fn handle(&self, message: MessageEnvelope) -> Result<Option<MessageEnvelope>> {
        // Echo the message back
        Ok(Some(message))
    }
}

/// Basic TCP network server
pub struct TcpNetworkServer {
    running: bool,
    address: Option<SocketAddr>,
    handler: Option<BasicMessageHandler>,
}

impl TcpNetworkServer {
    pub fn new() -> Self {
        Self {
            running: false,
            address: None,
            handler: None,
        }
    }
}

impl Default for TcpNetworkServer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NetworkServer for TcpNetworkServer {
    type Message = MessageEnvelope;
    type Handler = BasicMessageHandler;

    async fn start(&mut self, address: SocketAddr) -> Result<()> {
        // TODO: Implement actual TCP server
        self.address = Some(address);
        self.running = true;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.running = false;
        self.address = None;
        Ok(())
    }

    fn register_handler(&mut self, handler: Self::Handler) {
        self.handler = Some(handler);
    }

    fn is_running(&self) -> bool {
        self.running
    }
}
