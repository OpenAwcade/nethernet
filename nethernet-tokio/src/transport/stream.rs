use crate::addr::Addr;
use crate::error::{NethernetError, Result, SignalErrorCode};
use crate::protocol::constants::{RELIABLE_CHANNEL, UNRELIABLE_CHANNEL};
use crate::protocol::webrtc::{format_ice_candidate, parse_ice_candidate};
use crate::protocol::{Signal, SignalType};
use crate::session::Session;
use crate::signaling::Signaling;
use crate::transport::{ConnectionConfig, build_peer_connection};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use rand::Rng;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_util::io::StreamReader;
use tokio_util::sync::ReusableBoxFuture;
use webrtc::data_channel::{DataChannel, RTCDataChannelInit};
use webrtc::peer_connection::{
    RTCIceGatheringState, RTCPeerConnectionIceEvent, RTCSessionDescription,
};

/// Parses the error code of a `CONNECTERROR` signal.
pub(crate) fn parse_error_code(data: &str) -> SignalErrorCode {
    data.trim().parse::<u32>().map_or(
        SignalErrorCode::SignalingUnknownError,
        SignalErrorCode::from,
    )
}

struct CandidateHandler {
    candidate_tx: mpsc::UnboundedSender<webrtc::peer_connection::RTCIceCandidate>,
    gathering_tx: mpsc::UnboundedSender<RTCIceGatheringState>,
}

#[async_trait::async_trait]
impl webrtc::peer_connection::PeerConnectionEventHandler for CandidateHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        let _ = self.candidate_tx.send(event.candidate);
    }

    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        let _ = self.gathering_tx.send(state);
    }
}

fn spawn_candidate_forwarder<S: Signaling + 'static>(
    mut candidate_rx: mpsc::UnboundedReceiver<webrtc::peer_connection::RTCIceCandidate>,
    signaling: Arc<S>,
    connection_id: u64,
    remote_network_id: String,
) {
    tokio::spawn(async move {
        let mut index = 0usize;
        while let Some(candidate) = candidate_rx.recv().await {
            let data = format_ice_candidate(index, &candidate, "");
            index += 1;
            if let Err(error) = signaling
                .signal(Signal::candidate(
                    connection_id,
                    data,
                    remote_network_id.clone(),
                ))
                .await
            {
                tracing::debug!("failed to forward local ICE candidate: {}", error);
                break;
            }
        }
    });
}

async fn wait_for_gathering_complete(
    mut gathering_rx: mpsc::UnboundedReceiver<RTCIceGatheringState>,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, async move {
        while let Some(state) = gathering_rx.recv().await {
            if state == RTCIceGatheringState::Complete {
                return Ok(());
            }
        }
        Err(NethernetError::ConnectionClosed)
    })
    .await
    .map_err(|_| NethernetError::Timeout)??;
    Ok(())
}

struct SessionStream {
    session: Arc<Session>,
    recv_future: ReusableBoxFuture<'static, Result<Option<Bytes>>>,
}

impl Stream for SessionStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.recv_future.poll(cx) {
            Poll::Ready(result) => {
                let session = self.session.clone();
                self.recv_future.set(async move { session.recv().await });
                match result {
                    Ok(Some(data)) => Poll::Ready(Some(Ok(data))),
                    Ok(None) => Poll::Ready(None),
                    Err(error) => Poll::Ready(Some(Err(io::Error::other(error)))),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A byte stream backed by a NetherNet WebRTC data-channel session.
pub struct NethernetStream {
    session: Arc<Session>,
    reader: StreamReader<SessionStream, Bytes>,
    send_future: Option<ReusableBoxFuture<'static, Result<()>>>,
    shutdown_future: Option<ReusableBoxFuture<'static, Result<()>>>,
}

impl NethernetStream {
    /// Establish a connection to a remote NetherNet network.
    pub async fn connect<S: Signaling + 'static>(
        signaling: Arc<S>,
        remote_network_id: String,
    ) -> Result<Self> {
        Self::connect_with(signaling, remote_network_id, ConnectionConfig::default()).await
    }

    /// Establish a connection with explicit negotiation timeouts.
    pub async fn connect_with<S: Signaling + 'static>(
        signaling: Arc<S>,
        remote_network_id: String,
        config: ConnectionConfig,
    ) -> Result<Self> {
        let mut bytes = [0u8; 8];
        rand::rng().fill_bytes(&mut bytes);
        let connection_id = u64::from_le_bytes(bytes);
        let cancel_token = config.cancel_token.clone();
        let result = tokio::select! {
            _ = cancel_token.cancelled() => Err((None, NethernetError::ConnectionClosed)),
            result = Self::negotiate(&signaling, &remote_network_id, connection_id, config) => result,
        };
        match result {
            Ok(stream) => Ok(stream),
            Err((code, error)) => {
                if let Some(code) = code {
                    let _ = signaling
                        .signal(Signal::error(
                            connection_id,
                            code,
                            remote_network_id.clone(),
                        ))
                        .await;
                }
                Err(error)
            }
        }
    }

    async fn negotiate<S: Signaling + 'static>(
        signaling: &Arc<S>,
        remote_network_id: &str,
        connection_id: u64,
        config: ConnectionConfig,
    ) -> std::result::Result<Self, (Option<SignalErrorCode>, NethernetError)> {
        let (candidate_tx, candidate_rx) = mpsc::unbounded_channel();
        let (gathering_tx, gathering_rx) = mpsc::unbounded_channel();
        let handler = Arc::new(CandidateHandler {
            candidate_tx,
            gathering_tx,
        });
        let credentials = signaling
            .credentials()
            .await
            .map_err(|error| (Some(SignalErrorCode::SignalingTurnAuthFailed), error))?;
        let peer_connection = build_peer_connection(credentials.as_ref(), handler)
            .await
            .map_err(|error| (Some(SignalErrorCode::FailedToCreatePeerConnection), error))?;

        let reliable = peer_connection
            .create_data_channel(
                RELIABLE_CHANNEL,
                Some(RTCDataChannelInit {
                    ordered: true,
                    ..Default::default()
                }),
            )
            .await
            .map_err(|error| {
                (
                    Some(SignalErrorCode::FailedToCreatePeerConnection),
                    error.into(),
                )
            })?;
        let unreliable = peer_connection
            .create_data_channel(
                UNRELIABLE_CHANNEL,
                Some(RTCDataChannelInit {
                    max_retransmits: Some(0),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|error| {
                (
                    Some(SignalErrorCode::FailedToCreatePeerConnection),
                    error.into(),
                )
            })?;

        let offer = peer_connection
            .create_offer(None)
            .await
            .map_err(|error| (Some(SignalErrorCode::FailedToCreateOffer), error.into()))?;
        peer_connection
            .set_local_description(offer.clone())
            .await
            .map_err(|error| {
                (
                    Some(SignalErrorCode::FailedToSetLocalDescription),
                    error.into(),
                )
            })?;

        if !signaling.disable_trickle_ice() {
            spawn_candidate_forwarder(
                candidate_rx,
                signaling.clone(),
                connection_id,
                remote_network_id.to_string(),
            );
        }

        let offer_sdp = if signaling.disable_trickle_ice() {
            wait_for_gathering_complete(gathering_rx, config.timeouts.start)
                .await
                .map_err(|error| (Some(SignalErrorCode::FailedToCreateOffer), error))?;
            peer_connection
                .local_description()
                .await
                .ok_or((
                    Some(SignalErrorCode::FailedToCreateOffer),
                    NethernetError::InvalidState("missing local description".to_string()),
                ))?
                .sdp
        } else {
            offer.sdp
        };

        let mut signals = signaling.signals();
        signaling
            .signal(Signal::offer(
                connection_id,
                offer_sdp,
                remote_network_id.to_string(),
            ))
            .await
            .map_err(|error| (None, error))?;

        let mut pending_candidates = Vec::new();
        let answer = tokio::time::timeout(config.timeouts.negotiation, async {
            loop {
                let Some(signal) = signals.next().await else {
                    return Err((None, NethernetError::ConnectionClosed));
                };
                if signal.connection_id != connection_id || signal.network_id != remote_network_id {
                    continue;
                }
                match signal.signal_type {
                    SignalType::Answer => break Ok(signal.data),
                    SignalType::Candidate => {
                        if let Ok(candidate) = parse_ice_candidate(&signal.data) {
                            pending_candidates.push(candidate);
                        }
                    }
                    SignalType::Error => {
                        break Err((
                            None,
                            NethernetError::Signaled(parse_error_code(&signal.data)),
                        ));
                    }
                    SignalType::Offer => {
                        break Err((
                            Some(SignalErrorCode::IncomingConnectionIgnored),
                            NethernetError::Other("received offer while dialing".into()),
                        ));
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            (
                Some(SignalErrorCode::NegotiationTimeoutWaitingForResponse),
                NethernetError::Timeout,
            )
        })??;

        let remote_description = RTCSessionDescription::answer(answer).map_err(|error| {
            (
                Some(SignalErrorCode::FailedToSetRemoteDescription),
                NethernetError::WebRtc(error),
            )
        })?;
        peer_connection
            .set_remote_description(remote_description)
            .await
            .map_err(|error| {
                (
                    Some(SignalErrorCode::FailedToSetRemoteDescription),
                    NethernetError::WebRtc(error),
                )
            })?;
        for candidate in pending_candidates {
            let _ = peer_connection.add_ice_candidate(candidate).await;
        }

        wait_for_channel_open(reliable.clone(), config.timeouts.start)
            .await
            .map_err(|error| (Some(SignalErrorCode::Ice), error))?;
        wait_for_channel_open(unreliable.clone(), config.timeouts.start)
            .await
            .map_err(|error| (Some(SignalErrorCode::Ice), error))?;
        let local = Addr::new(signaling.network_id(), connection_id);
        let peer_for_signals = peer_connection.clone();
        let session = Arc::new(Session::new(
            peer_connection,
            local,
            Addr::new(remote_network_id.to_string(), connection_id),
        ));
        session
            .set_reliable_channel(reliable)
            .await
            .map_err(|error| (Some(SignalErrorCode::Ice), error))?;
        session
            .set_unreliable_channel(unreliable)
            .await
            .map_err(|error| (Some(SignalErrorCode::Ice), error))?;

        let session_for_signals = session.clone();
        let remote_id = remote_network_id.to_string();
        tokio::spawn(async move {
            loop {
                let signal = tokio::select! {
                    _ = session_for_signals.closed() => break,
                    signal = signals.next() => match signal {
                        Some(signal) => signal,
                        None => break,
                    },
                };
                if signal.connection_id != connection_id || signal.network_id != remote_id {
                    continue;
                }
                match signal.signal_type {
                    SignalType::Candidate => {
                        if let Ok(candidate) = parse_ice_candidate(&signal.data) {
                            let _ = peer_for_signals.add_ice_candidate(candidate).await;
                        }
                    }
                    SignalType::Error => {
                        let _ = session_for_signals.close().await;
                        break;
                    }
                    _ => {}
                }
            }
        });
        Ok(Self::from_session(session))
    }

    /// Construct a stream from an established session.
    pub fn from_session(session: Arc<Session>) -> Self {
        let clone = session.clone();
        let recv_future = ReusableBoxFuture::new(async move { clone.recv().await });
        Self {
            session: session.clone(),
            reader: StreamReader::new(SessionStream {
                session,
                recv_future,
            }),
            send_future: None,
            shutdown_future: None,
        }
    }

    pub async fn send(&self, data: Bytes) -> Result<()> {
        self.session.send(data).await
    }
    pub async fn send_unreliable(&self, data: Bytes) -> Result<()> {
        self.session.send_unreliable(data).await
    }
    pub async fn recv_unreliable(&self) -> Result<Option<Bytes>> {
        self.session.recv_unreliable().await
    }
    pub async fn recv(&self) -> Result<Option<Bytes>> {
        self.session.recv().await
    }
    pub async fn close(&self) -> Result<()> {
        self.session.close().await
    }
    pub async fn remote_addr(&self) -> Addr {
        self.session.remote_addr().await
    }
    pub async fn local_addr(&self) -> Addr {
        self.session.local_addr().await
    }
    pub fn session(&self) -> Arc<Session> {
        self.session.clone()
    }
}

async fn wait_for_channel_open(channel: Arc<dyn DataChannel>, timeout: Duration) -> Result<()> {
    tokio::time::timeout(timeout, async move {
        while let Some(event) = channel.poll().await {
            if matches!(event, webrtc::data_channel::DataChannelEvent::OnOpen) {
                return Ok(());
            }
        }
        Err(NethernetError::ConnectionClosed)
    })
    .await
    .map_err(|_| NethernetError::Timeout)??;
    Ok(())
}

impl AsyncRead for NethernetStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for NethernetStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(mut future) = self.send_future.take() {
            match future.poll(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
                Poll::Pending => {
                    self.send_future = Some(future);
                    return Poll::Pending;
                }
            }
        }
        let data = Bytes::copy_from_slice(buf);
        let length = data.len();
        let session = self.session.clone();
        let mut future = ReusableBoxFuture::new(async move { session.send(data).await });
        match future.poll(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
            Poll::Pending => self.send_future = Some(future),
        }
        Poll::Ready(Ok(length))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(mut future) = self.send_future.take() {
            match future.poll(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(io::Error::other(error))),
                Poll::Pending => {
                    self.send_future = Some(future);
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        if self.shutdown_future.is_none() {
            let session = self.session.clone();
            self.shutdown_future =
                Some(ReusableBoxFuture::new(async move { session.close().await }));
        }
        let mut future = self.shutdown_future.take().expect("shutdown future set");
        match future.poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(io::Error::other(error))),
            Poll::Pending => {
                self.shutdown_future = Some(future);
                Poll::Pending
            }
        }
    }
}
