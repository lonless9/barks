//! Network Common traits

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Trait for network message
pub trait NetworkMessage: Send + Sync + Clone {
    /// Get message type
    fn message_type(&self) -> &str;

    /// Serialize message to bytes
    fn to_bytes(&self) -> Result<Vec<u8>>;

    /// Deserialize message from bytes
    fn from_bytes(data: &[u8]) -> Result<Self>;
}

/// Trait for network client
#[async_trait]
pub trait NetworkClient: Send + Sync {
    type Message: NetworkMessage;

    /// Connect to server
    async fn connect(&mut self, address: SocketAddr) -> Result<()>;

    /// Send message
    async fn send(&mut self, message: Self::Message) -> Result<()>;

    /// Receive message
    async fn receive(&mut self) -> Result<Self::Message>;

    /// Close connection
    async fn close(&mut self) -> Result<()>;

    /// Check if connected
    fn is_connected(&self) -> bool;
}

/// Trait for network server
#[async_trait]
pub trait NetworkServer: Send + Sync {
    type Message: NetworkMessage;
    type Handler: MessageHandler<Self::Message>;

    /// Start server
    async fn start(&mut self, address: SocketAddr) -> Result<()>;

    /// Stop server
    async fn stop(&mut self) -> Result<()>;

    /// Register message handler
    fn register_handler(&mut self, handler: Self::Handler);

    /// Check if server is running
    fn is_running(&self) -> bool;
}

/// Trait for message handler
#[async_trait]
pub trait MessageHandler<M: NetworkMessage>: Send + Sync {
    /// Handle incoming message
    async fn handle(&self, message: M) -> Result<Option<M>>;
}

/// Trait for connection pool
#[async_trait]
pub trait ConnectionPool: Send + Sync {
    type Connection: NetworkClient;

    /// Get connection from pool
    async fn get_connection(&self, address: SocketAddr) -> Result<Self::Connection>;

    /// Return connection to pool
    async fn return_connection(&self, connection: Self::Connection) -> Result<()>;

    /// Get pool size
    fn pool_size(&self) -> usize;

    /// Close all connections
    async fn close_all(&self) -> Result<()>;
}

/// Trait for network transport
#[async_trait]
pub trait NetworkTransport: Send + Sync {
    type Message: NetworkMessage;

    /// Send message to address
    async fn send_to(&self, address: SocketAddr, message: Self::Message) -> Result<()>;

    /// Broadcast message to multiple addresses
    async fn broadcast(&self, addresses: &[SocketAddr], message: Self::Message) -> Result<()>;

    /// Start listening for incoming messages
    async fn start_listening(&self, address: SocketAddr) -> Result<()>;

    /// Stop listening
    async fn stop_listening(&self) -> Result<()>;
}
