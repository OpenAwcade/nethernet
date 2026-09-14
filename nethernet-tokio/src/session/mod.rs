use crate::addr::Addr;
use crate::error::{NethernetError, Result};
use crate::protocol::{Message, MessageSegment};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::PeerConnection;

/// Routes message segments received from a WebRTC data channel into the
/// session's reassembly buffer.
fn spawn_channel_reader(
    channel: Arc<dyn DataChannel>,
    buffer: Arc<Mutex<Message>>,
    tx: mpsc::Sender<Bytes>,
) {
    tokio::spawn(async move {
        while let Some(event) = channel.poll().await {
            let DataChannelEvent::OnMessage(message) = event else {
                continue;
            };
            let data = message.data.freeze();
            let data_len = data.len();
            match MessageSegment::decode(data.clone()) {
                Ok(segment) => {
                    let result = {
                        let mut buffer = buffer.lock().await;
                        buffer.add_segment(segment)
                    };
                    match result {
                        Ok(Some(complete_message)) => {
                            let _ = tx.send(complete_message).await;
                        }
                        Ok(None) => {}
                        Err(error) => tracing::warn!(
                            "failed to add segment to buffer: {:?}, data length: {}",
                            error,
                            data_len
                        ),
                    }
                }
                Err(error) => tracing::warn!(
                    "failed to decode message segment: {:?}, data length: {}",
                    error,
                    data_len
                ),
            }
        }
    });
}

/// WebRTC session manager.
pub struct Session {
    peer_connection: Arc<dyn PeerConnection>,
    local: Addr,
    remote: Arc<Mutex<Addr>>,
    reliable_channel: StdRwLock<Option<Arc<dyn DataChannel>>>,
    unreliable_channel: StdRwLock<Option<Arc<dyn DataChannel>>>,
    message_buffer: Arc<Mutex<Message>>,
    unreliable_buffer: Arc<Mutex<Message>>,
    packet_tx: mpsc::Sender<Bytes>,
    packet_rx: Arc<Mutex<mpsc::Receiver<Bytes>>>,
    unreliable_tx: mpsc::Sender<Bytes>,
    unreliable_rx: Arc<Mutex<mpsc::Receiver<Bytes>>>,
    closed: AtomicBool,
    close_token: CancellationToken,
}

impl Session {
    /// Creates a session backed by a 0.20.5 PeerConnection.
    pub fn new(peer_connection: Arc<dyn PeerConnection>, local: Addr, remote: Addr) -> Self {
        let (packet_tx, packet_rx) = mpsc::channel(128);
        let (unreliable_tx, unreliable_rx) = mpsc::channel(128);
        Self {
            peer_connection,
            local,
            remote: Arc::new(Mutex::new(remote)),
            reliable_channel: StdRwLock::new(None),
            unreliable_channel: StdRwLock::new(None),
            message_buffer: Arc::new(Mutex::new(Message::new())),
            unreliable_buffer: Arc::new(Mutex::new(Message::new())),
            packet_tx,
            packet_rx: Arc::new(Mutex::new(packet_rx)),
            unreliable_tx,
            unreliable_rx: Arc::new(Mutex::new(unreliable_rx)),
            closed: AtomicBool::new(false),
            close_token: CancellationToken::new(),
        }
    }

    /// Attach the reliable data channel.
    pub async fn set_reliable_channel(&self, channel: Arc<dyn DataChannel>) -> Result<()> {
        spawn_channel_reader(
            channel.clone(),
            self.message_buffer.clone(),
            self.packet_tx.clone(),
        );
        *self
            .reliable_channel
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(channel);
        Ok(())
    }

    /// Attach the unreliable data channel.
    pub async fn set_unreliable_channel(&self, channel: Arc<dyn DataChannel>) -> Result<()> {
        spawn_channel_reader(
            channel.clone(),
            self.unreliable_buffer.clone(),
            self.unreliable_tx.clone(),
        );
        *self
            .unreliable_channel
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(channel);
        Ok(())
    }

    /// Send a reliable message.
    pub async fn send(&self, data: Bytes) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(NethernetError::ConnectionClosed);
        }
        let channel = self
            .reliable_channel
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| NethernetError::DataChannel("Reliable channel not set".into()))?;
        for segment in Message::split_into_segments(data)? {
            channel
                .send(BytesMut::from(segment.encode().as_ref()))
                .await
                .map_err(|error| NethernetError::DataChannel(error.to_string()))?;
        }
        Ok(())
    }

    /// Send an unreliable message.
    pub async fn send_unreliable(&self, data: Bytes) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(NethernetError::ConnectionClosed);
        }
        let channel = self
            .unreliable_channel
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| NethernetError::DataChannel("Unreliable channel not set".into()))?;
        for segment in Message::split_into_segments(data)? {
            channel
                .send(BytesMut::from(segment.encode().as_ref()))
                .await
                .map_err(|error| NethernetError::DataChannel(error.to_string()))?;
        }
        Ok(())
    }

    /// Receive an unreliable message.
    pub async fn recv_unreliable(&self) -> Result<Option<Bytes>> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        Ok(self.unreliable_rx.lock().await.recv().await)
    }

    /// Receive a reliable message.
    pub async fn recv(&self) -> Result<Option<Bytes>> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        Ok(self.packet_rx.lock().await.recv().await)
    }

    /// Close the session and its PeerConnection.
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.close_token.cancel();
        let reliable_channel = self
            .reliable_channel
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(channel) = reliable_channel {
            let _ = channel.close().await;
        }
        let unreliable_channel = self
            .unreliable_channel
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(channel) = unreliable_channel {
            let _ = channel.close().await;
        }
        self.peer_connection
            .close()
            .await
            .map_err(NethernetError::from)
    }

    /// Local transport address.
    pub async fn local_addr(&self) -> Addr {
        self.local.clone()
    }

    /// Remote transport address.
    pub async fn remote_addr(&self) -> Addr {
        self.remote.lock().await.clone()
    }

    /// Wait until the session closes.
    pub async fn closed(&self) {
        self.close_token.cancelled().await;
    }

    /// Whether the session has closed.
    pub async fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}
