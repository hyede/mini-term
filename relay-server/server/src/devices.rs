use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket};
use mt_relay_protocol::{
    CommandFailReason, DesktopDevice, DesktopRejectReason, DesktopToRelay,
    MobileRejectReason, MobileToRelay, PairedMobile, RelayToDesktop, RelayToMobile,
    StartSessionFailReason, PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Notify};

use crate::{random_id, secret_eq, to_text, ConnSlot, RelayState, HANDSHAKE_TIMEOUT};

const REQUEST_TTL: Duration = Duration::from_secs(30);
const MAX_PENDING_PER_MOBILE: usize = 64;

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Grants {
    desktops: HashMap<String, DesktopGrant>,
    mobiles: HashMap<String, MobileGrant>,
}

#[derive(Clone, Serialize, Deserialize)]
struct DesktopGrant {
    name: String,
    device_key: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct MobileGrant {
    mobile_id: String,
    name: String,
}

struct DesktopConnection {
    slot: ConnSlot,
    manage_devices: bool,
}

struct MobileConnection {
    slot: ConnSlot,
    desktop_id: Option<String>,
    multi_device: bool,
    subscriptions: HashSet<String>,
}

struct PairingCode {
    code: String,
    issued_at: Instant,
}

struct Pending {
    credential: String,
    generation: u64,
    desktop_id: String,
    original_id: String,
    response_type: &'static str,
    pane_id: Option<String>,
    issued_at: Instant,
}

#[derive(Default)]
pub(super) struct Registry {
    grants: Grants,
    state_file: Option<PathBuf>,
    desktops: HashMap<String, DesktopConnection>,
    mobiles: HashMap<String, MobileConnection>,
    pairing_codes: HashMap<String, PairingCode>,
    pending: HashMap<String, Pending>,
    controls: HashMap<(String, String), String>,
}

impl Registry {
    pub(super) fn load(&mut self, path: &Path) -> io::Result<()> {
        let grants = match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Grants::default(),
            Err(error) => return Err(error),
        };
        self.grants = grants;
        self.state_file = Some(path.to_path_buf());
        Ok(())
    }

    fn save(&mut self, grants: Grants) -> io::Result<()> {
        if let Some(path) = &self.state_file {
            let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            std::fs::create_dir_all(parent)?;
            let temporary = parent.join(format!(".devices-{}.tmp", random_id()));
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let result = (|| {
                let mut file = options.open(&temporary)?;
                file.write_all(&serde_json::to_vec(&grants).map_err(io::Error::other)?)?;
                file.sync_all()?;
                std::fs::rename(&temporary, path)
            })();
            if result.is_err() {
                let _ = std::fs::remove_file(&temporary);
            }
            result?;
        }
        self.grants = grants;
        Ok(())
    }

    fn send_desktop(&self, desktop_id: &str, message: &RelayToDesktop) {
        if let Some(connection) = self.desktops.get(desktop_id) {
            connection.slot.send(to_text(message));
        }
    }

    fn send_mobile(&self, credential: &str, desktop_id: &str, mut value: serde_json::Value) {
        if let Some(connection) = self.mobiles.get(credential) {
            if connection.multi_device {
                value["desktopId"] = desktop_id.into();
            }
            connection.slot.send(to_text(&value));
        }
    }

    fn lists(&self) {
        let mut devices: Vec<_> = self.grants.desktops.iter().map(|(desktop_id, grant)| {
            DesktopDevice {
                desktop_id: desktop_id.clone(), name: grant.name.clone(),
                online: self.desktops.contains_key(desktop_id),
            }
        }).collect();
        devices.sort_by(|first, second| first.desktop_id.cmp(&second.desktop_id));
        for connection in self.mobiles.values().filter(|connection| connection.multi_device) {
            connection.slot.send(to_text(&RelayToMobile::DesktopList {
                devices: devices.clone(), selected_desktop_id: connection.desktop_id.clone(),
            }));
        }
    }

    fn pairing_updates(&self) {
        let mut devices: Vec<_> = self.grants.mobiles.values().map(|grant| PairedMobile {
            mobile_id: grant.mobile_id.clone(), name: grant.name.clone(),
        }).collect();
        devices.sort_by(|first, second| first.mobile_id.cmp(&second.mobile_id));
        for connection in self.desktops.values() {
            connection.slot.send(to_text(&RelayToDesktop::PairingUpdate { paired: !devices.is_empty() }));
            if connection.manage_devices {
                connection.slot.send(to_text(&RelayToDesktop::PairedDevices { devices: devices.clone() }));
            }
        }
    }

    fn subscribed(&self, desktop_id: &str, pane_id: &str) -> bool {
        self.mobiles.values().any(|connection| {
            connection.desktop_id.as_deref() == Some(desktop_id)
                && connection.subscriptions.contains(pane_id)
        })
    }

    fn drop_mobile(&mut self, credential: &str) {
        if let Some(connection) = self.mobiles.remove(credential) {
            if let Some(desktop_id) = connection.desktop_id {
                for pane_id in connection.subscriptions {
                    if !self.subscribed(&desktop_id, &pane_id) {
                        self.send_desktop(&desktop_id, &RelayToDesktop::UnsubscribePane { pane_id });
                    }
                }
            }
        }
        self.pending.retain(|_, pending| pending.credential != credential);
        self.controls.retain(|_, owner| owner != credential);
    }

    fn presence(&self, credential: &str) {
        if let Some(connection) = self.mobiles.get(credential) {
            let desktop_id = connection.desktop_id.as_deref().unwrap_or("");
            self.send_mobile(credential, desktop_id, serde_json::to_value(RelayToMobile::Presence {
                desktop_online: self.desktops.contains_key(desktop_id),
            }).unwrap());
        }
    }
}

fn name(value: &str, fallback: &str) -> String {
    let value: String = value.trim().chars().filter(|character| !character.is_control())
        .take(64).collect();
    if value.is_empty() { fallback.into() } else { value }
}

async fn first_frame(socket: &mut WebSocket) -> Option<serde_json::Value> {
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, socket.recv()).await {
        Ok(Some(Ok(Message::Text(text)))) => serde_json::from_str(&text).ok(),
        _ => None,
    }
}

async fn pump(
    mut socket: WebSocket, mut outbound: mpsc::Receiver<Message>, overflow: Arc<Notify>,
    receive: impl Fn(serde_json::Value),
) {
    loop {
        tokio::select! {
            _ = overflow.notified() => break,
            frame = outbound.recv() => match frame {
                Some(frame) => {
                    let close = matches!(frame, Message::Close(_));
                    if socket.send(frame).await.is_err() || close { break; }
                }
                None => break,
            },
            frame = socket.recv() => match frame {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(value) = serde_json::from_str(&text) { receive(value); }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {},
            },
        }
    }
}

pub(super) async fn desktop(mut socket: WebSocket, state: RelayState) {
    let Some(value) = first_frame(&mut socket).await else { return; };
    let Ok(DesktopToRelay::Hello {
        protocol_version, desktop_key, desktop_id, desktop_name, device_key,
    }) = serde_json::from_value(value.clone()) else { return; };
    let rejection = if protocol_version != PROTOCOL_VERSION {
        Some(DesktopRejectReason::VersionMismatch)
    } else {
        state.authenticate_desktop(&desktop_key).err()
    };
    if let Some(reason) = rejection {
        let version = reason == DesktopRejectReason::VersionMismatch;
        let _ = socket.send(to_text(&RelayToDesktop::HelloReject {
            reason, expected_version: version.then_some(PROTOCOL_VERSION),
            actual_version: version.then_some(protocol_version),
        })).await;
        let _ = socket.send(Message::Close(None)).await;
        return;
    }
    if desktop_id.is_empty() || desktop_id.len() > 128 || device_key.is_empty() || device_key.len() > 256 {
        let _ = socket.send(to_text(&RelayToDesktop::HelloReject {
            reason: DesktopRejectReason::InvalidKey, expected_version: None, actual_version: None,
        })).await;
        return;
    }
    let generation = state.next_generation();
    let (sender, receiver) = state.outbound_channel();
    let overflow = Arc::new(Notify::new());
    let registered = {
        let mut registry = state.devices.lock().unwrap();
        let valid = registry.grants.desktops.get(&desktop_id)
            .is_none_or(|grant| secret_eq(&grant.device_key, &device_key));
        if !valid {
            false
        } else {
            let mut grants = registry.grants.clone();
            grants.desktops.insert(desktop_id.clone(), DesktopGrant {
                name: name(&desktop_name, &desktop_id), device_key,
            });
            if registry.save(grants).is_err() {
                eprintln!("[relay] desktop authorization persistence failed");
                false
            } else {
                let connection = DesktopConnection {
                    slot: ConnSlot { generation, tx: sender, overflow: overflow.clone() },
                    manage_devices: value["manageDevices"].as_bool().unwrap_or(false),
                };
                if let Some(old) = registry.desktops.insert(desktop_id.clone(), connection) {
                    old.slot.send(Message::Close(None));
                }
                registry.send_desktop(&desktop_id, &RelayToDesktop::HelloAck { protocol_version: PROTOCOL_VERSION });
                registry.send_desktop(&desktop_id, &RelayToDesktop::PairingUpdate {
                    paired: !registry.grants.mobiles.is_empty(),
                });
                if registry.desktops[&desktop_id].manage_devices {
                    let devices = registry.grants.mobiles.values().map(|grant| PairedMobile {
                        mobile_id: grant.mobile_id.clone(), name: grant.name.clone(),
                    }).collect();
                    registry.send_desktop(&desktop_id, &RelayToDesktop::PairedDevices { devices });
                }
                for (credential, mobile) in &registry.mobiles {
                    if mobile.desktop_id.as_deref() == Some(&desktop_id) {
                        registry.presence(credential);
                    }
                }
                if registry.mobiles.values().any(|mobile| mobile.desktop_id.as_deref() == Some(&desktop_id)) {
                    registry.send_desktop(&desktop_id, &RelayToDesktop::SessionsSnapshotRequest);
                }
                let panes: HashSet<_> = registry.mobiles.values()
                    .filter(|mobile| mobile.desktop_id.as_deref() == Some(&desktop_id))
                    .flat_map(|mobile| mobile.subscriptions.iter().cloned()).collect();
                for pane_id in panes {
                    registry.send_desktop(&desktop_id, &RelayToDesktop::SubscribePane { pane_id });
                }
                registry.lists();
                true
            }
        }
    };
    if !registered {
        let _ = socket.send(to_text(&RelayToDesktop::HelloReject {
            reason: DesktopRejectReason::InvalidKey, expected_version: None, actual_version: None,
        })).await;
        let _ = socket.send(Message::Close(None)).await;
        return;
    }
    eprintln!("[relay] desktop connected (gen {generation})");
    pump(socket, receiver, overflow, |value| {
        let mut registry = state.devices.lock().unwrap();
        if registry.desktops.get(&desktop_id).is_some_and(|connection| connection.slot.generation == generation) {
            registry.desktop_message(&desktop_id, value, state.code_ttl);
        }
    }).await;
    let mut registry = state.devices.lock().unwrap();
    if registry.desktops.get(&desktop_id).is_some_and(|connection| connection.slot.generation == generation) {
        registry.desktops.remove(&desktop_id);
        registry.pending.retain(|_, pending| pending.desktop_id != desktop_id);
        registry.controls.retain(|(device, _), _| device != &desktop_id);
        for credential in registry.mobiles.keys() { registry.presence(credential); }
        registry.lists();
    }
    eprintln!("[relay] desktop disconnected (gen {generation})");
}

pub(super) async fn mobile(mut socket: WebSocket, state: RelayState) {
    let Some(value) = first_frame(&mut socket).await else { return; };
    let Ok(MobileToRelay::Hello {
        protocol_version, pairing_code, credential, desktop_id, device_name, multi_device,
    }) = serde_json::from_value(value) else { return; };
    if protocol_version != PROTOCOL_VERSION {
        let _ = socket.send(to_text(&RelayToMobile::HelloReject { reason: MobileRejectReason::VersionMismatch })).await;
        let _ = socket.send(Message::Close(None)).await;
        return;
    }
    let generation = state.next_generation();
    let (sender, receiver) = state.outbound_channel();
    let overflow = Arc::new(Notify::new());
    let registered = {
        let mut registry = state.devices.lock().unwrap();
        let authenticated = registry.authenticate_mobile(pairing_code, credential, &device_name, state.code_ttl);
        authenticated.map(|(credential, issued, paired_desktop)| {
            registry.drop_mobile(&credential);
            let selected = desktop_id.filter(|id| registry.grants.desktops.contains_key(id))
                .or(paired_desktop)
                .or_else(|| registry.grants.desktops.keys().min().cloned());
            registry.mobiles.insert(credential.clone(), MobileConnection {
                slot: ConnSlot { generation, tx: sender, overflow: overflow.clone() },
                desktop_id: selected.clone(), multi_device, subscriptions: HashSet::new(),
            });
            registry.mobiles[&credential].slot.send(to_text(&RelayToMobile::HelloAck {
                protocol_version: PROTOCOL_VERSION, credential: issued.then(|| credential.clone()),
            }));
            registry.lists();
            registry.presence(&credential);
            if issued { registry.pairing_updates(); }
            if let Some(id) = selected {
                registry.send_desktop(&id, &RelayToDesktop::SessionsSnapshotRequest);
            }
            credential
        })
    };
    let credential = match registered {
        Ok(credential) => credential,
        Err(reason) => {
            let _ = socket.send(to_text(&RelayToMobile::HelloReject { reason })).await;
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };
    eprintln!("[relay] mobile connected (gen {generation})");
    pump(socket, receiver, overflow, |value| {
        let mut registry = state.devices.lock().unwrap();
        if registry.mobiles.get(&credential).is_some_and(|connection| connection.slot.generation == generation) {
            registry.mobile_message(&credential, value);
        }
    }).await;
    let mut registry = state.devices.lock().unwrap();
    if registry.mobiles.get(&credential).is_some_and(|connection| connection.slot.generation == generation) {
        registry.drop_mobile(&credential);
    }
    eprintln!("[relay] mobile disconnected (gen {generation})");
}

impl Registry {
    fn authenticate_mobile(
        &mut self, code: Option<String>, credential: Option<String>, device_name: &str, ttl: Duration,
    ) -> Result<(String, bool, Option<String>), MobileRejectReason> {
        if let Some(code) = code {
            let desktop_id = self.pairing_codes.iter().find(|(_, active)| {
                secret_eq(&active.code, &code) && active.issued_at.elapsed() <= ttl
            }).map(|(id, _)| id.clone()).ok_or(MobileRejectReason::InvalidPairingCode)?;
            let credential = random_id();
            let mut grants = self.grants.clone();
            grants.mobiles.insert(credential.clone(), MobileGrant {
                mobile_id: random_id(), name: name(device_name, "Mobile"),
            });
            self.save(grants).map_err(|_| {
                eprintln!("[relay] mobile authorization persistence failed");
                MobileRejectReason::InvalidPairingCode
            })?;
            self.pairing_codes.remove(&desktop_id);
            Ok((credential, true, Some(desktop_id)))
        } else if let Some(credential) = credential {
            if self.grants.mobiles.contains_key(&credential) {
                if let Some(old) = self.mobiles.get(&credential) { old.slot.send(Message::Close(None)); }
                Ok((credential, false, None))
            } else { Err(MobileRejectReason::InvalidCredential) }
        } else { Err(MobileRejectReason::MissingAuth) }
    }
}

impl Registry {
    fn desktop_message(&mut self, desktop_id: &str, value: serde_json::Value, ttl: Duration) {
        let Ok(message) = serde_json::from_value::<DesktopToRelay>(value.clone()) else { return; };
        match message {
            DesktopToRelay::Hello { .. } => return,
            DesktopToRelay::RequestPairingCode => {
                self.pairing_codes.retain(|_, code| code.issued_at.elapsed() <= ttl);
                let code = random_id();
                self.pairing_codes.insert(desktop_id.into(), PairingCode { code: code.clone(), issued_at: Instant::now() });
                self.send_desktop(desktop_id, &RelayToDesktop::PairingCode { code });
                return;
            }
            DesktopToRelay::ResetPairing | DesktopToRelay::RevokeMobile { .. } => {
                let reset = matches!(message, DesktopToRelay::ResetPairing);
                let mut grants = self.grants.clone();
                match message {
                    DesktopToRelay::RevokeMobile { mobile_id } => grants.mobiles.retain(|_, grant| grant.mobile_id != mobile_id),
                    _ => grants.mobiles.clear(),
                }
                if self.save(grants).is_err() {
                    eprintln!("[relay] authorization revocation persistence failed");
                    return;
                }
                if reset { self.pairing_codes.clear(); }
                let revoked: Vec<_> = self.mobiles.keys().filter(|credential| !self.grants.mobiles.contains_key(*credential)).cloned().collect();
                for credential in revoked {
                    self.mobiles[&credential].slot.send(to_text(&RelayToMobile::Revoked));
                    self.mobiles[&credential].slot.send(Message::Close(None));
                    self.drop_mobile(&credential);
                }
                self.pairing_updates();
                return;
            }
            _ => {}
        }
        if serde_json::from_value::<RelayToMobile>(value.clone()).is_err() { return; }
        let message_type = value["type"].as_str().unwrap_or("");
        if matches!(message_type, "commandReceipt" | "startSessionReceipt" | "mirrorHistory" | "sessionList" | "sessionContent") {
            let field = if message_type == "commandReceipt" { "commandId" } else { "requestId" };
            let route_id = value[field].as_str().map(str::to_owned).or_else(|| {
                let matches: Vec<_> = self.pending.iter().filter(|(_, pending)| {
                    pending.desktop_id == desktop_id && pending.response_type == message_type
                        && pending.pane_id.as_deref() == value["paneId"].as_str()
                        && self.mobiles.get(&pending.credential).is_some_and(|mobile| !mobile.multi_device)
                }).map(|(id, _)| id.clone()).collect();
                (matches.len() == 1).then(|| matches[0].clone())
            });
            let Some(route_id) = route_id else { return; };
            let Some(pending) = self.pending.get(&route_id) else { return; };
            if pending.desktop_id != desktop_id || pending.response_type != message_type
                || pending.issued_at.elapsed() > REQUEST_TTL
                || (pending.pane_id.is_some() && pending.pane_id.as_deref() != value["paneId"].as_str()) {
                return;
            }
            let pending = self.pending.remove(&route_id).unwrap();
            if !self.mobiles.get(&pending.credential).is_some_and(|mobile| {
                mobile.slot.generation == pending.generation && mobile.desktop_id.as_deref() == Some(desktop_id)
            }) { return; }
            let mut response = value.clone();
            if message_type == "mirrorHistory" { response.as_object_mut().unwrap().remove("requestId"); }
            else { response[field] = pending.original_id.into(); }
            self.send_mobile(&pending.credential, desktop_id, response);
            return;
        }
        let pane_id = value["paneId"].as_str();
        let mirror_message = matches!(message_type, "mirrorSnapshot" | "mirrorAppend" | "paneClosed");
        for (credential, mobile) in &self.mobiles {
            if mobile.desktop_id.as_deref() == Some(desktop_id)
                && (!mirror_message || pane_id.is_some_and(|pane| mobile.subscriptions.contains(pane))) {
                self.send_mobile(credential, desktop_id, value.clone());
            }
        }
        if message_type == "paneClosed" {
            if let Some(pane_id) = pane_id {
                for mobile in self.mobiles.values_mut().filter(|mobile| mobile.desktop_id.as_deref() == Some(desktop_id)) {
                    mobile.subscriptions.remove(pane_id);
                }
                self.controls.remove(&(desktop_id.into(), pane_id.into()));
            }
        }
    }

    fn mobile_message(&mut self, credential: &str, mut value: serde_json::Value) {
        let Ok(message) = serde_json::from_value::<MobileToRelay>(value.clone()) else { return; };
        if matches!(message, MobileToRelay::Hello { .. }) { return; }
        if let MobileToRelay::SelectDesktop { desktop_id } = message {
            if !self.grants.desktops.contains_key(&desktop_id) { return; }
            let connection = self.mobiles.get_mut(credential).unwrap();
            let old = connection.desktop_id.replace(desktop_id.clone());
            let subscriptions = std::mem::take(&mut connection.subscriptions);
            if let Some(old) = old {
                for pane_id in subscriptions {
                    if !self.subscribed(&old, &pane_id) {
                        self.send_desktop(&old, &RelayToDesktop::UnsubscribePane { pane_id });
                    }
                }
            }
            self.pending.retain(|_, pending| pending.credential != credential);
            self.controls.retain(|_, owner| owner != credential);
            self.lists();
            self.presence(credential);
            self.send_desktop(&desktop_id, &RelayToDesktop::SessionsSnapshotRequest);
            return;
        }
        let Some(desktop_id) = self.mobiles[credential].desktop_id.clone() else { return; };
        match &message {
            MobileToRelay::SubscribePane { pane_id } => {
                let already = self.subscribed(&desktop_id, pane_id);
                if !self.mobiles.get_mut(credential).unwrap().subscriptions.insert(pane_id.clone()) { return; }
                let request = if already { RelayToDesktop::MirrorSnapshotRequest { pane_id: pane_id.clone() } }
                    else { RelayToDesktop::SubscribePane { pane_id: pane_id.clone() } };
                self.send_desktop(&desktop_id, &request);
                return;
            }
            MobileToRelay::UnsubscribePane { pane_id } => {
                self.mobiles.get_mut(credential).unwrap().subscriptions.remove(pane_id);
                if !self.subscribed(&desktop_id, pane_id) {
                    self.send_desktop(&desktop_id, &RelayToDesktop::UnsubscribePane { pane_id: pane_id.clone() });
                }
                if self.controls.get(&(desktop_id.clone(), pane_id.clone())).is_some_and(|owner| owner == credential) {
                    self.controls.remove(&(desktop_id, pane_id.clone()));
                }
                return;
            }
            MobileToRelay::ReleaseControl { pane_id } => {
                if self.controls.get(&(desktop_id.clone(), pane_id.clone())).is_some_and(|owner| owner == credential) {
                    self.controls.remove(&(desktop_id.clone(), pane_id.clone()));
                }
                self.send_mobile(credential, &desktop_id, serde_json::to_value(RelayToMobile::ControlReleased {
                    pane_id: pane_id.clone(),
                }).unwrap());
                return;
            }
            _ => {}
        }
        if !self.desktops.contains_key(&desktop_id) {
            match message {
                MobileToRelay::MobileCommand { pane_id, command_id, .. }
                | MobileToRelay::AnswerQuestion { pane_id, command_id, .. } => {
                    self.send_mobile(credential, &desktop_id, serde_json::to_value(RelayToMobile::CommandReceipt {
                        pane_id, command_id, ok: false, reason: Some(CommandFailReason::DesktopOffline),
                    }).unwrap());
                }
                MobileToRelay::StartAiSession { request_id, .. }
                | MobileToRelay::ResumeAiSession { request_id, .. } => {
                    self.send_mobile(credential, &desktop_id, serde_json::to_value(RelayToMobile::StartSessionReceipt {
                        request_id, ok: false, pane_id: None, reason: Some(StartSessionFailReason::DesktopOffline),
                    }).unwrap());
                }
                MobileToRelay::RequestSessionList { request_id, project_id, .. } => {
                    self.send_mobile(credential, &desktop_id, serde_json::to_value(RelayToMobile::SessionList {
                        request_id, project_id, sessions: Vec::new(), has_more: false,
                        reason: Some(StartSessionFailReason::DesktopOffline),
                    }).unwrap());
                }
                MobileToRelay::RequestSessionContent { request_id, project_id, agent, session_id, .. } => {
                    self.send_mobile(credential, &desktop_id, serde_json::to_value(RelayToMobile::SessionContent {
                        revision: None,
                        request_id, project_id, agent, session_id, messages: Vec::new(), has_more: false,
                        reason: Some(StartSessionFailReason::DesktopOffline),
                    }).unwrap());
                }
                _ => {}
            }
            return;
        }
        let routing = match &message {
            MobileToRelay::MobileCommand { command_id, pane_id, .. }
            | MobileToRelay::AnswerQuestion { command_id, pane_id, .. } => {
                let key = (desktop_id.clone(), pane_id.clone());
                if self.controls.get(&key).is_some_and(|owner| owner != credential) {
                    self.send_mobile(credential, &desktop_id, serde_json::to_value(RelayToMobile::CommandReceipt {
                        pane_id: pane_id.clone(), command_id: command_id.clone(), ok: false,
                        reason: Some(CommandFailReason::ControlBusy),
                    }).unwrap());
                    return;
                }
                self.controls.insert(key, credential.into());
                Some(("commandId", command_id.clone(), "commandReceipt", Some(pane_id.clone())))
            }
            MobileToRelay::StartAiSession { request_id, .. }
            | MobileToRelay::ResumeAiSession { request_id, .. } => Some(("requestId", request_id.clone(), "startSessionReceipt", None)),
            MobileToRelay::RequestSessionList { request_id, .. } => Some(("requestId", request_id.clone(), "sessionList", None)),
            MobileToRelay::RequestSessionContent { request_id, .. } => Some(("requestId", request_id.clone(), "sessionContent", None)),
            MobileToRelay::RequestMirrorHistory { pane_id, .. } => {
                if !self.mobiles[credential].subscriptions.contains(pane_id) { return; }
                Some(("requestId", String::new(), "mirrorHistory", Some(pane_id.clone())))
            }
            _ => None,
        };
        if let Some((field, original_id, response_type, pane_id)) = routing {
            self.pending.retain(|_, pending| pending.issued_at.elapsed() <= REQUEST_TTL);
            if self.pending.values().filter(|pending| pending.credential == credential).count() >= MAX_PENDING_PER_MOBILE {
                self.mobiles[credential].slot.overflow.notify_one();
                return;
            }
            let modern = self.mobiles[credential].multi_device;
            let route_id = random_id();
            if modern || response_type != "mirrorHistory" { value[field] = route_id.clone().into(); }
            self.pending.insert(route_id, Pending {
                credential: credential.into(), generation: self.mobiles[credential].slot.generation,
                desktop_id: desktop_id.clone(), original_id, response_type, pane_id, issued_at: Instant::now(),
            });
        }
        if let Ok(message) = serde_json::from_value::<RelayToDesktop>(value) {
            self.send_desktop(&desktop_id, &message);
        }
    }
}
