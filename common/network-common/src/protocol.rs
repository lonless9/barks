//! Network protocol definitions

use serde::{Deserialize, Serialize};
use crate::traits::NetworkMessage;
use anyhow::Result;

/// Standard network message envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub message_id: String,
    pub message_type: String,
    pub sender: String,
    pub recipient: String,
    pub timestamp: u64,
    pub payload: Vec<u8>,
}

impl NetworkMessage for MessageEnvelope {
    fn message_type(&self) -> &str {
        &self.message_type
    }
    
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    
    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// RPC request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub request_id: String,
    pub method: String,
    pub params: Vec<u8>,
}

impl NetworkMessage for RpcRequest {
    fn message_type(&self) -> &str {
        "rpc_request"
    }
    
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    
    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// RPC response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub request_id: String,
    pub success: bool,
    pub result: Option<Vec<u8>>,
    pub error: Option<String>,
}

impl NetworkMessage for RpcResponse {
    fn message_type(&self) -> &str {
        "rpc_response"
    }
    
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    
    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// Heartbeat message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    pub node_id: String,
    pub timestamp: u64,
    pub status: String,
}

impl NetworkMessage for HeartbeatMessage {
    fn message_type(&self) -> &str {
        "heartbeat"
    }
    
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    
    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}
