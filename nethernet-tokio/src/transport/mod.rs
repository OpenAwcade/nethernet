//! WebRTC transport facade backed by `webrtc 0.20`'s PeerConnection API.

pub mod listener;
pub mod stream;

pub use listener::NethernetListener;
pub use stream::NethernetStream;

use crate::credentials::Credentials;
use crate::error::{NethernetError, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
    RTCIceServer,
};

/// Options applied while negotiating and establishing a connection.
#[derive(Debug, Clone, Default)]
pub struct ConnectionConfig {
    /// Timeouts of each negotiation step.
    pub timeouts: Timeouts,

    /// Cancels the negotiation when triggered.
    pub cancel_token: CancellationToken,
}

/// Timeouts applied while negotiating and establishing a connection.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    /// Time to wait for the answer of the remote connection.
    pub negotiation: Duration,
    /// Time to wait for the first candidate signaled by the remote connection.
    pub candidate: Duration,
    /// Time to wait for the connection state to become connected.
    pub start: Duration,
    /// Time to wait for remote data channels.
    pub channel: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            negotiation: Duration::from_secs(15),
            candidate: Duration::from_secs(5),
            start: Duration::from_secs(15),
            channel: Duration::from_secs(10),
        }
    }
}

/// Build a 0.20.5 PeerConnection using the credentials returned by signaling.
pub(crate) async fn build_peer_connection(
    credentials: Option<&Credentials>,
    handler: Arc<dyn PeerConnectionEventHandler>,
) -> Result<Arc<dyn PeerConnection>> {
    let ice_servers = credentials
        .into_iter()
        .flat_map(|credentials| credentials.ice_servers.iter())
        .map(|server| RTCIceServer {
            urls: server.urls.clone(),
            username: server.username.clone(),
            credential: server.password.clone(),
        })
        .collect();

    let configuration = RTCConfigurationBuilder::new()
        .with_ice_servers(ice_servers)
        .build();
    let peer_connection = PeerConnectionBuilder::new()
        .with_configuration(configuration)
        .with_handler(handler)
        .with_udp_addrs(vec!["0.0.0.0:0"])
        .build()
        .await
        .map_err(NethernetError::from)?;

    Ok(Arc::new(peer_connection))
}
