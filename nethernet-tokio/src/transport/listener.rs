use crate::addr::Addr;
use crate::error::{NethernetError, Result, SignalErrorCode};
use crate::protocol::constants::{RELIABLE_CHANNEL, UNRELIABLE_CHANNEL};
use crate::protocol::webrtc::{format_ice_candidate, parse_ice_candidate};
use crate::protocol::{Signal, SignalType};
use crate::session::Session;
use crate::signaling::Signaling;
use crate::transport::{ConnectionConfig, build_peer_connection};
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use webrtc::data_channel::DataChannel;
use webrtc::peer_connection::{
    RTCIceGatheringState, RTCPeerConnectionIceEvent, RTCSessionDescription,
};

type ConnectionKey = (String, u64);
type SignalDispatchers = Arc<Mutex<HashMap<ConnectionKey, mpsc::UnboundedSender<Signal>>>>;

struct AnswerHandler {
    candidate_tx: mpsc::UnboundedSender<webrtc::peer_connection::RTCIceCandidate>,
    gathering_tx: mpsc::UnboundedSender<RTCIceGatheringState>,
    data_channel_tx: mpsc::UnboundedSender<Arc<dyn DataChannel>>,
}

#[async_trait::async_trait]
impl webrtc::peer_connection::PeerConnectionEventHandler for AnswerHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        let _ = self.candidate_tx.send(event.candidate);
    }

    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        let _ = self.gathering_tx.send(state);
    }

    async fn on_data_channel(&self, channel: Arc<dyn DataChannel>) {
        let _ = self.data_channel_tx.send(channel);
    }
}

/// Signals a negotiation error back to the remote connection.
async fn signal_error<S: Signaling>(
    signaling: &Arc<S>,
    connection_id: u64,
    network_id: String,
    code: SignalErrorCode,
) {
    let _ = signaling
        .signal(Signal::error(connection_id, code, network_id))
        .await;
}

/// NetherNet listener accepting WebRTC data-channel sessions.
pub struct NethernetListener<S: Signaling> {
    incoming: mpsc::UnboundedReceiver<Arc<Session>>,
    local_addr: Addr,
    cancel_token: CancellationToken,
    signal_handler_task: JoinHandle<()>,
    _phantom: PhantomData<S>,
}

impl<S: Signaling + 'static> NethernetListener<S> {
    /// Bind a listener to the signaling network.
    pub async fn bind(signaling: S) -> Result<Self> {
        Self::bind_with(signaling, ConnectionConfig::default()).await
    }

    /// Bind a listener with explicit negotiation timeouts.
    pub async fn bind_with(signaling: S, config: ConnectionConfig) -> Result<Self> {
        let signaling = Arc::new(signaling);
        let local_addr = Addr::network(signaling.network_id());
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let dispatchers = Arc::new(Mutex::new(HashMap::new()));
        let cancel_token = CancellationToken::new();
        let signal_handler_task = Self::start_signal_handler(
            signaling,
            incoming_tx,
            dispatchers,
            cancel_token.clone(),
            config,
        );
        Ok(Self {
            incoming: incoming_rx,
            local_addr,
            cancel_token,
            signal_handler_task,
            _phantom: PhantomData,
        })
    }

    fn start_signal_handler(
        signaling: Arc<S>,
        incoming_tx: mpsc::UnboundedSender<Arc<Session>>,
        dispatchers: SignalDispatchers,
        cancel_token: CancellationToken,
        config: ConnectionConfig,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut signals = signaling.signals();
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break,
                    signal = signals.next() => {
                        let Some(signal) = signal else { break };
                        if signal.signal_type == SignalType::Offer {
                            let key = (signal.network_id.clone(), signal.connection_id);
                            let (signal_tx, signal_rx) = mpsc::unbounded_channel();
                            dispatchers.lock().await.insert(key.clone(), signal_tx);
                            let signaling = signaling.clone();
                            let incoming_tx = incoming_tx.clone();
                            let dispatchers = dispatchers.clone();
                            let config = config.clone();
                            tokio::spawn(async move {
                                if let Err((code, error)) = Self::answer_offer(
                                    signal.clone(),
                                    &signaling,
                                    &incoming_tx,
                                    &dispatchers,
                                    config,
                                    signal_rx,
                                ).await {
                                    tracing::debug!("failed to answer offer: {}", error);
                                    dispatchers.lock().await.remove(&key);
                                    if let Some(code) = code {
                                        signal_error(&signaling, signal.connection_id, signal.network_id, code).await;
                                    }
                                }
                            });
                        } else {
                            let key = (signal.network_id.clone(), signal.connection_id);
                            if let Some(tx) = dispatchers.lock().await.get(&key) {
                                let _ = tx.send(signal);
                            }
                        }
                    }
                }
            }
        })
    }

    async fn answer_offer(
        signal: Signal,
        signaling: &Arc<S>,
        incoming_tx: &mpsc::UnboundedSender<Arc<Session>>,
        dispatchers: &SignalDispatchers,
        config: ConnectionConfig,
        mut signal_rx: mpsc::UnboundedReceiver<Signal>,
    ) -> std::result::Result<(), (Option<SignalErrorCode>, NethernetError)> {
        let connection_id = signal.connection_id;
        let network_id = signal.network_id.clone();
        let remote_offer = RTCSessionDescription::offer(signal.data).map_err(|error| {
            (
                Some(SignalErrorCode::FailedToSetRemoteDescription),
                NethernetError::WebRtc(error),
            )
        })?;

        let key = (network_id.clone(), connection_id);

        let (candidate_tx, candidate_rx) = mpsc::unbounded_channel();
        let (gathering_tx, gathering_rx) = mpsc::unbounded_channel();
        let (data_channel_tx, mut data_channel_rx) = mpsc::unbounded_channel();
        let handler = Arc::new(AnswerHandler {
            candidate_tx,
            gathering_tx,
            data_channel_tx,
        });
        let credentials = signaling
            .credentials()
            .await
            .map_err(|error| (Some(SignalErrorCode::SignalingTurnAuthFailed), error))?;
        let peer_connection = build_peer_connection(credentials.as_ref(), handler)
            .await
            .map_err(|error| (Some(SignalErrorCode::FailedToCreatePeerConnection), error))?;

        peer_connection
            .set_remote_description(remote_offer)
            .await
            .map_err(|error| {
                (
                    Some(SignalErrorCode::FailedToSetRemoteDescription),
                    NethernetError::WebRtc(error),
                )
            })?;

        let answer = peer_connection.create_answer(None).await.map_err(|error| {
            (
                Some(SignalErrorCode::FailedToCreateAnswer),
                NethernetError::WebRtc(error),
            )
        })?;
        peer_connection
            .set_local_description(answer.clone())
            .await
            .map_err(|error| {
                (
                    Some(SignalErrorCode::FailedToSetLocalDescription),
                    NethernetError::WebRtc(error),
                )
            })?;

        let candidate_signaling = signaling.clone();
        let candidate_remote = network_id.clone();
        if !signaling.disable_trickle_ice() {
            tokio::spawn(async move {
                let mut index = 0usize;
                let mut candidate_rx = candidate_rx;
                while let Some(candidate) = candidate_rx.recv().await {
                    let result = candidate_signaling
                        .signal(Signal::candidate(
                            connection_id,
                            format_ice_candidate(index, &candidate, ""),
                            candidate_remote.clone(),
                        ))
                        .await;
                    index += 1;
                    if result.is_err() {
                        break;
                    }
                }
            });
        }

        let answer_sdp = if signaling.disable_trickle_ice() {
            wait_for_gathering_complete(gathering_rx, config.timeouts.start)
                .await
                .map_err(|error| (Some(SignalErrorCode::FailedToCreateAnswer), error))?;
            peer_connection
                .local_description()
                .await
                .ok_or((
                    Some(SignalErrorCode::FailedToCreateAnswer),
                    NethernetError::InvalidState("missing local description".to_string()),
                ))?
                .sdp
        } else {
            answer.sdp
        };

        let pc_for_candidates = peer_connection.clone();
        let candidate_cancel = CancellationToken::new();
        let candidate_cancel_for_task = candidate_cancel.clone();
        let dispatchers_for_candidates = dispatchers.clone();
        let key_for_candidates = key.clone();
        tokio::spawn(async move {
            loop {
                let signal = tokio::select! {
                    _ = candidate_cancel_for_task.cancelled() => break,
                    signal = signal_rx.recv() => match signal {
                        Some(signal) => signal,
                        None => break,
                    },
                };
                match signal.signal_type {
                    SignalType::Candidate => {
                        if let Ok(candidate) = parse_ice_candidate(&signal.data) {
                            let _ = pc_for_candidates.add_ice_candidate(candidate).await;
                        }
                    }
                    SignalType::Error => break,
                    _ => {}
                }
            }
            dispatchers_for_candidates
                .lock()
                .await
                .remove(&key_for_candidates);
        });

        signaling
            .signal(Signal::answer(
                connection_id,
                answer_sdp,
                network_id.clone(),
            ))
            .await
            .map_err(|error| (None, error))?;

        let (reliable, unreliable) =
            wait_for_channels(&mut data_channel_rx, config.timeouts.channel)
                .await
                .map_err(|error| {
                    (
                        Some(SignalErrorCode::NegotiationTimeoutWaitingForAccept),
                        error,
                    )
                })?;
        wait_for_channel_open(reliable.clone(), config.timeouts.channel)
            .await
            .map_err(|error| {
                (
                    Some(SignalErrorCode::NegotiationTimeoutWaitingForAccept),
                    error,
                )
            })?;
        wait_for_channel_open(unreliable.clone(), config.timeouts.channel)
            .await
            .map_err(|error| {
                (
                    Some(SignalErrorCode::NegotiationTimeoutWaitingForAccept),
                    error,
                )
            })?;
        let session = Arc::new(Session::new(
            peer_connection,
            Addr::new(signaling.network_id(), connection_id),
            Addr::new(network_id, connection_id),
        ));
        session
            .set_reliable_channel(reliable)
            .await
            .map_err(|error| (Some(SignalErrorCode::Ice), error))?;
        session
            .set_unreliable_channel(unreliable)
            .await
            .map_err(|error| (Some(SignalErrorCode::Ice), error))?;
        incoming_tx
            .send(session.clone())
            .map_err(|_| (None, NethernetError::ConnectionClosed))?;

        let session_for_candidates = session.clone();
        tokio::spawn(async move {
            session_for_candidates.closed().await;
            candidate_cancel.cancel();
        });
        Ok(())
    }

    /// Accept the next inbound session.
    pub async fn accept(&mut self) -> Result<Arc<Session>> {
        self.incoming
            .recv()
            .await
            .ok_or(NethernetError::ConnectionClosed)
    }

    /// Close the listener.
    pub async fn close(&mut self) -> Result<()> {
        self.cancel_token.cancel();
        self.incoming.close();
        self.signal_handler_task.abort();
        Ok(())
    }

    /// Local network address.
    pub fn local_addr(&self) -> &Addr {
        &self.local_addr
    }
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

async fn wait_for_channels(
    data_channel_rx: &mut mpsc::UnboundedReceiver<Arc<dyn DataChannel>>,
    timeout: Duration,
) -> Result<(Arc<dyn DataChannel>, Arc<dyn DataChannel>)> {
    tokio::time::timeout(timeout, async {
        let mut reliable = None;
        let mut unreliable = None;
        while reliable.is_none() || unreliable.is_none() {
            let channel = data_channel_rx
                .recv()
                .await
                .ok_or(NethernetError::ConnectionClosed)?;
            let label = channel
                .label()
                .await
                .map_err(|error| NethernetError::DataChannel(error.to_string()))?;
            if label == RELIABLE_CHANNEL {
                reliable = Some(channel);
            } else if label == UNRELIABLE_CHANNEL {
                unreliable = Some(channel);
            }
        }
        Ok((
            reliable.expect("reliable channel set"),
            unreliable.expect("unreliable channel set"),
        ))
    })
    .await
    .map_err(|_| NethernetError::Timeout)?
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

impl<S: Signaling> Drop for NethernetListener<S> {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.signal_handler_task.abort();
    }
}

impl<S: Signaling + 'static + Unpin> Stream for NethernetListener<S> {
    type Item = Arc<Session>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().incoming.poll_recv(cx)
    }
}
